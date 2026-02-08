"""
メール処理統合スクリプト（重複防止機能付き）
IMAP → Gemini解析 → BigQuery挿入
"""
import sys
import os
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
from dotenv import load_dotenv
import json
import re
import hashlib
from datetime import datetime, timezone

# 環境変数読み込み
load_dotenv()

# IMAP設定
IMAP_SERVER = os.getenv('IMAP_SERVER')
IMAP_PORT = int(os.getenv('IMAP_PORT', 993))
IMAP_USER = os.getenv('IMAP_USER')
IMAP_PASSWORD = os.getenv('IMAP_PASSWORD')

# Gemini API
import google.generativeai as genai
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
genai.configure(api_key=GOOGLE_API_KEY)

# Excel処理
import openpyxl
from io import BytesIO

# BigQuery
from google.cloud import bigquery
from google.oauth2 import service_account

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
if not GCP_PROJECT_ID:
    raise RuntimeError("GCP_PROJECT_ID is not set")

if not BIGQUERY_DATASET:
    raise RuntimeError("BIGQUERY_DATASET is not set")

BIGQUERY_TABLE_ENGINEERS = 'EngineerData'
BIGQUERY_TABLE_PROJECTS = 'ProjectData'

# BigQuery認証（GitHub Actions対応）
gcp_json_str = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
if gcp_json_str:
    # GitHub Actionsの場合（JSON文字列）
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(gcp_json_str)
    )
else:
    # ローカルの場合（JSONファイル）
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS
    )


def generate_mail_fingerprint(sender_email, subject, body, sent_at):
    """
    メールの一意性を判定するfingerprintを生成
    
    Args:
        sender_email: 送信者メールアドレス
        subject: 件名
        body: 本文（先頭500文字を使用）
        sent_at: 送信日時（ISO形式）
    
    Returns:
        SHA-256ハッシュ文字列（64文字）
    """
    # 本文は先頭500文字のみ使用（署名・フッター差分を吸収）
    body_part = body[:500] if body else ""
    
    # 結合して一意の文字列を作成
    base = f"{sender_email}|{subject}|{body_part}|{sent_at}"
    
    # SHA-256ハッシュ化
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def decode_mime_header(header_text):
    """MIMEヘッダーをデコード"""
    if not header_text:
        return ''
    
    decoded_parts = decode_header(header_text)
    decoded_text = ''
    
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            try:
                decoded_text += part.decode(encoding or 'utf-8', errors='ignore')
            except:
                decoded_text += part.decode('iso-2022-jp', errors='ignore')
        else:
            decoded_text += str(part)
    
    return decoded_text


def fetch_recent_emails(limit=50):
    """最新メールを50件取得（既読・未読問わず）+ 送信日時を取得"""
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(IMAP_USER, IMAP_PASSWORD)
        mail.select('INBOX')
        
        # 全メール検索
        status, message_ids = mail.search(None, 'ALL')
        
        if status != 'OK' or not message_ids[0]:
            mail.close()
            mail.logout()
            return []
        
        email_ids = message_ids[0].split()
        
        # 最新からlimit件取得
        email_ids = email_ids[-limit:] if len(email_ids) > limit else email_ids
        
        emails = []
        
        for email_id in reversed(email_ids):
            status, msg_data = mail.fetch(email_id, '(RFC822)')
            
            if status != 'OK':
                continue
            
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            
            # 件名
            subject = decode_mime_header(msg.get('Subject', ''))
            
            # 送信者
            from_header = msg.get('From', '')
            sender_name, sender_email_addr = email.utils.parseaddr(from_header)
            sender_name = decode_mime_header(sender_name)
            
            # ★★★ 送信日時を取得（重複防止の要） ★★★
            date_header = msg.get("Date")
            sent_at = ""
            if date_header:
                try:
                    sent_at = parsedate_to_datetime(date_header).astimezone(timezone.utc).isoformat()
                except:
                    sent_at = ""
            
            # 本文取得
            body = ''
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get('Content-Disposition'))
                    
                    if content_type == 'text/plain' and 'attachment' not in content_disposition:
                        try:
                            payload = part.get_payload(decode=True)
                            body = payload.decode('utf-8', errors='ignore')
                            break
                        except:
                            pass
            else:
                try:
                    payload = msg.get_payload(decode=True)
                    body = payload.decode('utf-8', errors='ignore')
                except:
                    body = str(msg.get_payload())
            
            # 添付ファイル処理
            attachments = []
            for part in msg.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                
                filename = part.get_filename()
                
                if filename:
                    filename = decode_mime_header(filename)
                    
                    # Excel拡張子をチェック
                    if filename.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                        data = part.get_payload(decode=True)
                        size = len(data) if data else 0
                        
                        attachments.append({
                            'filename': filename,
                            'data': data,
                            'size': size
                        })
            
            emails.append({
                'email_id': email_id.decode(),
                'subject': subject,
                'sender': f"{sender_name} <{sender_email_addr}>",
                'sender_name': sender_name,
                'sender_email': sender_email_addr,
                'sent_at': sent_at,  # ★追加
                'body': body,
                'attachments': attachments
            })
        
        mail.close()
        mail.logout()
        
        return emails
        
    except Exception as e:
        print(f"❌ メール取得エラー: {e}")
        return []


def classify_and_extract_with_gemini(email_body, email_subject=""):
    """Gemini APIでメール解析"""
    
    # プロンプトからmainTextの出力を除外（トークン節約）
    prompt = f"""以下のメールを分析し、その内容が「案件情報（要員募集）」なのか「人材情報（技術者紹介）」なのかを厳密に判断し、該当するJSON形式で返してください。

【メール件名】
{email_subject}

【メール本文】
{email_body}

判定基準:
1. 「案件情報 (project)」: システム開発の案件への参画依頼、エンジニアの募集、案件概要、商流、単価などの情報が含まれる場合。キーワード(案件, 募集, 要員, 相談)
2. 「人材情報 (engineer)」: 特定の技術者（実名またはイニシャル）の紹介、スキルシートの添付、稼働可能日、経験年数、単価などの情報が含まれる場合。キーワード(人材, 紹介, 技術者, イニシャル, 稼働可)
3. 「その他 (other)」: 上記のどちらにも該当しない場合。

【案件情報(project)の場合のJSON】
{{
  "type": "project",
  "location": "勤務地",
  "period": "期間",
  "price": 単価(数値のみ。100万なら100, 70-80万なら80),
  "requiredSkills": "必須スキル（言語、DB、工程など）",
  "senderName": "署名から担当者名を抽出",
  "senderCompany": "署名から会社名を抽出"
}}

【人材情報(engineer)の場合のJSON】
{{
  "type": "engineer",
  "engineerName": "エンジニア名 (イニシャル)",
  "mainSkills": "主要スキル (言語、フレームワーク等)",
  "yearsOfExperience": 経験年数(数値のみ、不明なら0),
  "monthlyRate": 希望単価(数値のみ。80万なら80、800,000なら800000ではなく80のように適切なスケールで数値化。本文に合わせる),
  "availableFrom": "稼働開始可能日",
  "gender": "性別",
  "age": 年齢(数値のみ),
  "nearestStation": "最寄駅",
  "senderName": "署名から営業担当者名を抽出",
  "senderCompany": "署名から会社名を抽出"
}}

【その他の場合】
{{
  "type": "other",
  "senderName": "名刺や署名から送信者名を抽出",
  "senderCompany": "会社名を抽出"
}}

ルール:
- JSON形式のみ出力（説明文・コメント不要）
- 数値項目は整数のみ（単位や記号を除く）
- 不明な項目は空文字("")または0
- senderNameとsenderCompanyは必ずメール末尾の署名部分から抽出すること
- エンジニア名は本文中から抽出 (イニシャルのみでも可)
- 案件と人材が混在している場合は、より主要な方（または最初に記述されている方）を優先してください。"""
    
    model_names = [
        'models/gemini-2.0-flash'
    ]
    
    import time
    max_retries = 3
    base_delay = 5
    
    for model_name in model_names:
        for attempt in range(max_retries):
            try:
                model = genai.GenerativeModel(model_name)
                
                generation_config = {
                    'max_output_tokens': 8192,
                    'temperature': 0.1,
                    'top_p': 0.8,
                    'top_k': 40,
                    'response_mime_type': 'application/json'
                }
                
                response = model.generate_content(prompt, generation_config=generation_config)
                gemini_text = response.text
                
                # JSONクリーニング
                cleaned_text = re.sub(r'```json\s*', '', gemini_text)
                cleaned_text = re.sub(r'```\s*', '', cleaned_text)
                cleaned_text = cleaned_text.strip()
                
                extracted = json.loads(cleaned_text)
                
                # リスト形式で返ってきた場合の対応
                if isinstance(extracted, list):
                    if len(extracted) > 0:
                        extracted = extracted[0]
                    else:
                        print(f"    ⚠️  {model_name} エラー: 空のリストが返されました")
                        continue
                
                # 数値変換
                if extracted.get('type') == 'project':
                    if extracted.get('price'):
                        try:
                            extracted['price'] = int(str(extracted['price']).replace(',', ''))
                        except:
                            extracted['price'] = 0
                            
                elif extracted.get('type') == 'engineer':
                    if extracted.get('monthlyRate'):
                        try:
                            extracted['monthlyRate'] = int(str(extracted['monthlyRate']).replace(',', ''))
                        except:
                            extracted['monthlyRate'] = 0
                    
                    if extracted.get('yearsOfExperience'):
                        try:
                            extracted['yearsOfExperience'] = int(extracted['yearsOfExperience'])
                        except:
                            extracted['yearsOfExperience'] = 0
                    
                    if extracted.get('age'):
                        try:
                            extracted['age'] = int(extracted['age'])
                        except:
                            extracted['age'] = 0
                
                # Python側で本文を付与（トークン節約のためプロンプトからは除外）
                extracted['mainText'] = email_body
                if not email_body:
                     print("    ⚠️  警告: メール本文が空です")
                else:
                     print(f"    ℹ️  メール本文付与完了 (文字数: {len(email_body)})")
                
                return extracted
                
            except json.JSONDecodeError as e:
                print(f"    ⚠️  {model_name} JSONパースエラー: {e}")
                if 'gemini_text' in locals():
                    print(f"    Gemini出力: {gemini_text[:200]}...")
                # JSONエラーはリトライしても直らない可能性が高いが、念のため次のモデルへ
                break 
            except Exception as e:
                # 429エラーなどの場合はリトライ
                if "429" in str(e) or "quota" in str(e).lower():
                    delay = base_delay * (2 ** attempt)
                    print(f"    ⚠️  レート制限 (429)。{delay}秒後にリトライします... ({attempt+1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    print(f"    ⚠️  {model_name} エラー: {e}")
                    break
    
    print(f"    ❌ すべてのモデルで失敗")
    return None


def convert_to_bigquery_format(extracted_data, email_subject, fingerprint, sent_at, file_url="", excel_skills=None):
    """BigQuery形式に変換（fingerprint追加）"""
    
    data_type = extracted_data.get('type')
    
    if data_type == 'engineer':
        data = {
            'fingerprint': fingerprint,  # ★追加
            'sent_at': sent_at,  # ★追加
            'engineer_name': extracted_data.get('engineerName', ''),
            'main_skills': extracted_data.get('mainSkills', ''),
            'years_of_experience': extracted_data.get('yearsOfExperience', 0),
            'monthly_rate': extracted_data.get('monthlyRate', 0),
            'available_from': extracted_data.get('availableFrom', ''),
            'gender': extracted_data.get('gender', ''),
            'age': extracted_data.get('age', 0),
            'nearest_station': extracted_data.get('nearestStation', ''),
            'main_text': extracted_data.get('mainText', ''),
            'subject': email_subject,
            'sender_name': extracted_data.get('senderName', ''),
            'sender_company': extracted_data.get('senderCompany', ''),
            'fileURL': file_url,
            'extracted_at': datetime.now(timezone.utc).isoformat()
        }
        
        # excel_skills を追加（配列形式）
        if excel_skills:
            data['excel_skills'] = excel_skills
        
        return data
    elif data_type == 'project':
        return {
            'fingerprint': fingerprint,  # ★追加
            'sent_at': sent_at,  # ★追加
            'project_name': email_subject,  # 案件名はメール件名をそのまま使用
            'location': extracted_data.get('location', ''),
            'period': extracted_data.get('period', ''),
            'price': extracted_data.get('price', 0),
            'required_skills': extracted_data.get('requiredSkills', ''),
            'main_text': extracted_data.get('mainText', ''),
            'subject': email_subject,
            'sender_name': extracted_data.get('senderName', ''),
            'sender_company': extracted_data.get('senderCompany', ''),
            'fileURL': file_url,
            'extracted_at': datetime.now(timezone.utc).isoformat()
        }
    else:
        return None


def extract_excel_content(excel_data):
    """Excelファイルの中身をテキスト化"""
    try:
        wb = openpyxl.load_workbook(BytesIO(excel_data), data_only=True)
        sheet = wb.active
        
        all_text = []
        for row in sheet.iter_rows(values_only=True):
            row_text = ' | '.join([str(cell) for cell in row if cell is not None])
            if row_text.strip():
                all_text.append(row_text)
        
        return '\n'.join(all_text)
        
    except Exception as e:
        print(f"    ❌ Excel読み込みエラー: {e}")
        return None


def extract_skills_from_excel(excel_text):
    """GeminiでExcelからスキル情報を抽出"""
    
    prompt = f"""以下のExcelデータからエンジニアのスキル情報を全て抽出してください。

{excel_text}

以下のJSON形式で出力:
{{
  "excel_skills": ["スキル1", "スキル2", "スキル3", ...],
  "additional_info": {{
    "certifications": ["資格1", "資格2", ...],
    "projects": ["プロジェクト1", "プロジェクト2", ...],
    "other": "その他の有用な情報"
  }}
}}

ルール:
- excel_skillsは配列形式
- プログラミング言語、フレームワーク、ツール、技術など全て含める
- 重複は除外
- JSON形式のみ出力（説明文不要）"""
    
    model_names = [
        'models/gemini-2.0-flash'
    ]
    
    for model_name in model_names:
        try:
            model = genai.GenerativeModel(model_name)
            
            generation_config = {
                'max_output_tokens': 8192,
                'temperature': 0.1,
                'top_p': 0.8,
                'top_k': 40,
                'response_mime_type': 'application/json'
            }
            
            response = model.generate_content(prompt, generation_config=generation_config)
            gemini_text = response.text
            
            # JSONクリーニング
            cleaned_text = re.sub(r'```json\s*', '', gemini_text)
            cleaned_text = re.sub(r'```\s*', '', cleaned_text)
            cleaned_text = cleaned_text.strip()
            
            extracted = json.loads(cleaned_text)
            
            return extracted
            
        except Exception as e:
            continue
    
    return None


def fingerprint_exists(client, table_id, fingerprint):
    """
    BigQueryでfingerprintが既に存在するかチェック
    
    Args:
        client: BigQueryクライアント
        table_id: テーブルID（フルパス）
        fingerprint: チェックするfingerprint
    
    Returns:
        True: 存在する（重複）
        False: 存在しない（新規）
    """
    query = f"""
    SELECT 1
    FROM `{table_id}`
    WHERE fingerprint = @fingerprint
    LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("fingerprint", "STRING", fingerprint)
        ]
    )
    
    try:
        result = client.query(query, job_config=job_config).result()
        return result.total_rows > 0
    except Exception as e:
        # テーブルが存在しない場合などはFalseを返す
        print(f"    ⚠️  重複チェックエラー（新規とみなす）: {e}")
        return False


def insert_to_bigquery(data, data_type):
    """BigQueryに挿入"""
    try:
        client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
        
        if data_type == 'engineer':
            table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_ENGINEERS}"
        else:
            table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_PROJECTS}"
        
        # 新規挿入（重複チェックは呼び出し側で実施済み）
        errors = client.insert_rows_json(table_id, [data])
        
        if errors:
            print(f"  ❌ BigQuery挿入エラー: {errors}")
            return False
        else:
            return True
            
    except Exception as e:
        print(f"  ❌ BigQuery接続エラー: {e}")
        return False


def main():
    """メイン処理"""
    
    print("=" * 60)
    print("メール処理統合実行（重複防止機能付き）")
    print("=" * 60)
    
    # 最新メール取得
    print("\n【最新メール取得中...】")
    emails = fetch_recent_emails(limit=50)
    
    if not emails:
        print("メールは見つかりませんでした")
        return
    
    print(f"取得メール数: {len(emails)}件")
    
    processed_count = 0
    engineer_count = 0
    project_count = 0
    other_count = 0
    skipped_count = 0  # ★追加
    
    for i, email_data in enumerate(emails, 1):
        print(f"\n{'=' * 60}")
        print(f"【メール {i}/{len(emails)}】")
        print(f"{'=' * 60}")
        print(f"件名: {email_data['subject']}")
        print(f"送信者: {email_data['sender']}")
        print(f"送信日時: {email_data['sent_at']}")
        
        # ★★★ fingerprint生成 ★★★
        fingerprint = generate_mail_fingerprint(
            email_data['sender_email'],
            email_data['subject'],
            email_data['body'],
            email_data.get('sent_at', '')
        )
        print(f"fingerprint: {fingerprint[:16]}...")
        
        # ★★★ 早期重複チェック（Gemini呼び出し前） ★★★
        print("\n  🔍 重複チェック中...")
        try:
            client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
            
            # 両テーブルをチェック
            engineer_table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_ENGINEERS}"
            project_table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_PROJECTS}"
            
            if fingerprint_exists(client, engineer_table_id, fingerprint) or \
               fingerprint_exists(client, project_table_id, fingerprint):
                print(f"  ⏭️  既処理メール（fingerprint一致）- Gemini呼び出しスキップ")
                skipped_count += 1
                continue
        except Exception as e:
            print(f"  ⚠️  重複チェックエラー: {e}")
            # エラー時は処理を続行（安全側に倒す）
        
        # Gemini解析（重複がない場合のみ実行）
        print("\n  🤖 Gemini解析中...")
        try:
            extracted = classify_and_extract_with_gemini(email_data['body'], email_data['subject'])
            
            if not extracted:
                print("  ❌ 解析失敗: Geminiがレスポンスを返しませんでした")
                print(f"  メール本文（最初の200文字）: {email_data['body'][:200]}...")
                continue
        except Exception as e:
            print(f"  ❌ 解析エラー: {e}")
            import traceback
            traceback.print_exc()
            continue
        
        print(f"  ✅ 判定: {extracted.get('type')}")
        
        if extracted.get('type') == 'other':
            print("  → その他メール（スキップ）")
            other_count += 1
            continue
        
        # Excel添付ファイル処理（人材メールのみ）
        excel_skills = []
        if extracted.get('type') == 'engineer' and email_data.get('attachments'):
            print(f"\n  📎 Excel添付ファイル: {len(email_data['attachments'])}件")
            
            for attachment in email_data['attachments']:
                print(f"    ファイル: {attachment['filename']}")
                
                # Excelをテキスト化
                excel_text = extract_excel_content(attachment['data'])
                
                if excel_text:
                    print(f"    🤖 Excel解析中...")
                    excel_data = extract_skills_from_excel(excel_text)
                    
                    if excel_data and excel_data.get('excel_skills'):
                        excel_skills.extend(excel_data['excel_skills'])
                        print(f"    ✅ スキル抽出: {len(excel_data['excel_skills'])}件")
                        print(f"       {', '.join(excel_data['excel_skills'][:5])}...")
        
        # BigQuery形式に変換（fingerprint追加）
        bq_data = convert_to_bigquery_format(
            extracted, 
            email_data['subject'],
            fingerprint,  # ★追加
            email_data['sent_at'],  # ★追加
            "",
            excel_skills if excel_skills else None
        )
        
        if not bq_data:
            continue
        
        # BigQuery挿入
        print(f"  💾 BigQuery挿入中...")
        success = insert_to_bigquery(bq_data, extracted.get('type'))
        
        if success:
            print(f"  ✅ 挿入成功")
            processed_count += 1
            
            if extracted.get('type') == 'engineer':
                engineer_count += 1
                print(f"     テーブル: EngineerData")
                print(f"     エンジニア名: {bq_data.get('engineer_name')}")
                print(f"     スキル: {bq_data.get('main_skills')}")
                if excel_skills:
                    print(f"     Excelスキル: {len(excel_skills)}件")
            else:
                project_count += 1
                print(f"     テーブル: ProjectData")
                print(f"     案件名: {bq_data.get('project_name')}")
                print(f"     必須スキル: {bq_data.get('required_skills')}")
    
    # 結果サマリー
    print(f"\n{'=' * 60}")
    print("【処理結果】")
    print(f"{'=' * 60}")
    print(f"処理済み: {processed_count}件")
    print(f"  エンジニア情報: {engineer_count}件")
    print(f"  案件情報: {project_count}件")
    print(f"重複スキップ: {skipped_count}件")  # ★追加
    print(f"その他: {other_count}件")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
