import pandas as pd
import requests
import json
from datetime import datetime
import time
import os
import glob
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except ImportError:
    from requests.packages.urllib3.util.retry import Retry

# Configuration
INPUT_FILE_PATH = r"E:\clickup attachments\Input\PM_App_Export_September-23-2025.xlsx"
OUTPUT_FILE_PATH = r"E:\clickup attachments\dataoutput\clickupoutput.xlsx"
ATTACHMENTS_BASE_PATH = r"C:\Users\Abhishekgs\Desktop\ClickupData"
CLICKUP_API_TOKEN = "pk_100804363_VPAQ8T8R1OCVFPTHTXI3PJLIO4MZB4RP"

# ClickUp API headers
headers = {
    "Authorization": CLICKUP_API_TOKEN,
    "Content-Type": "application/json"
}

def create_session_with_retries():
    """Create a requests session with retry strategy"""
    session = requests.Session()
    
    # Define retry strategy with compatibility for different urllib3 versions
    try:
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],  # New parameter name
            backoff_factor=1
        )
    except TypeError:
        # Fallback for older versions
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],  # Old parameter name
            backoff_factor=1
        )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def test_api_connectivity():
    """Test if we can connect to ClickUp API"""
    print("🔍 Testing API connectivity...")
    try:
        # Simple approach without retry session for the test
        url = "https://api.clickup.com/api/v2/user"
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            user_data = response.json()
            username = user_data.get('user', {}).get('username', 'Unknown')
            print(f"✅ API connectivity successful! Connected as: {username}")
            return True
        else:
            print(f"❌ API connectivity failed: {response.status_code} - {response.text}")
            return False
    except requests.exceptions.Timeout:
        print("❌ API connectivity test timed out")
        return False
    except requests.exceptions.ConnectionError:
        print("❌ API connectivity test failed: Connection error")
        return False
    except Exception as e:
        print(f"❌ API connectivity test failed: {str(e)}")
        return False

def convert_date_to_timestamp(date_str):
    """Convert date string to Unix timestamp in milliseconds"""
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                dt = datetime.strptime(str(date_str), fmt)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
        dt = pd.to_datetime(date_str)
        return int(dt.timestamp() * 1000)
    except:
        return None

def get_status_id(status_name, list_id):
    """Get status ID from status name"""
    status_mapping = {
        'to do': 'to do',
        'in progress': 'in progress', 
        'completed': 'complete',
        'complete': 'complete',
        'done': 'complete'
    }
    return status_mapping.get(status_name.lower(), status_name.lower())

def get_priority_value(priority_str):
    """Convert priority string to ClickUp priority value"""
    if pd.isna(priority_str) or priority_str == '':
        return None
    
    priority_mapping = {
        'urgent': 1,
        'high': 2,
        'normal': 3,
        'low': 4
    }
    return priority_mapping.get(str(priority_str).lower(), 3)

def find_attachments_for_task(task_id):
    """Find all attachments for a given task ID"""
    task_folder = os.path.join(ATTACHMENTS_BASE_PATH, str(task_id))
    attachments = []
    
    if os.path.exists(task_folder):
        file_patterns = ['*.*']
        for pattern in file_patterns:
            files = glob.glob(os.path.join(task_folder, pattern))
            attachments.extend(files)
    
    return attachments

def upload_attachment_to_clickup(clickup_task_id, file_path):
    """Upload an attachment to a ClickUp task"""
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}"
    
    url = f"https://api.clickup.com/api/v2/task/{clickup_task_id}/attachment"
    
    upload_headers = {
        "Authorization": CLICKUP_API_TOKEN
    }
    
    try:
        file_name = os.path.basename(file_path)
        
        with open(file_path, 'rb') as file:
            files = {
                'attachment': (file_name, file, 'application/octet-stream')
            }
            
            print(f"   📎 Uploading attachment: {file_name}")
            response = requests.post(url, headers=upload_headers, files=files, timeout=60)
            
            if response.status_code == 200:
                attachment_response = response.json()
                attachment_id = attachment_response.get('id', 'Unknown')
                print(f"   ✅ Attachment uploaded successfully! Attachment ID: {attachment_id}")
                return True, attachment_id
            else:
                error_msg = f"Attachment upload error: {response.status_code} - {response.text}"
                print(f"   ❌ {error_msg}")
                return False, error_msg
                
    except Exception as e:
        error_msg = f"Attachment upload exception: {str(e)}"
        print(f"   ❌ {error_msg}")
        return False, error_msg

def create_clickup_task(row):
    """Create a task in ClickUp and return the response"""
    list_id = str(row['list_id'])
    
    # Prepare the task data
    task_data = {
        "name": str(row['name']),
        "description": str(row['description']) if pd.notna(row['description']) else "",
        "assignees": [int(row['assignee ID'])] if pd.notna(row['assignee ID']) else [],
        "status": get_status_id(str(row['status']), list_id) if pd.notna(row['status']) else None,
        "priority": get_priority_value(row['priority']),
        "due_date": convert_date_to_timestamp(row['due_date']),
        "time_estimate": int(float(row['time_estimate']) * 3600000) if pd.notna(row['time_estimate']) else None,
        "start_date": convert_date_to_timestamp(row['start_date_time']),
        "notify_all": False,
        "parent": None,
        "links_to": None,
        "check_required_custom_fields": True
    }
    
    # Remove None values
    task_data = {k: v for k, v in task_data.items() if v is not None}
    
    # API endpoint
    url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
    
    try:
        print(f"🔄 Making API request to create task...")
        print(f"   Task data: {json.dumps(task_data, indent=2)}")
        
        # Use simple requests with timeout
        response = requests.post(url, headers=headers, json=task_data, timeout=60)
        
        if response.status_code == 200:
            print(f"✅ Task creation successful!")
            return response.json(), None
        else:
            error_msg = f"Error creating task: {response.status_code} - {response.text}"
            print(f"❌ {error_msg}")
            return None, error_msg
            
    except requests.exceptions.Timeout:
        error_msg = "Request timed out while creating task"
        print(f"❌ {error_msg}")
        return None, error_msg
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Connection error while creating task: {str(e)}"
        print(f"❌ {error_msg}")
        return None, error_msg
    except Exception as e:
        error_msg = f"Exception creating task: {str(e)}"
        print(f"❌ {error_msg}")
        return None, error_msg

def add_comment_to_task(clickup_task_id, comment_text):
    """Add a comment to the created task using ClickUp task ID"""
    if pd.isna(comment_text) or comment_text == '':
        return True, None
    
    url = f"https://api.clickup.com/api/v2/task/{clickup_task_id}/comment"
    
    comment_data = {
        "comment_text": str(comment_text),
        "assignee": None,
        "notify_all": False
    }
    
    comment_headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": CLICKUP_API_TOKEN
    }
    
    try:
        print(f"   💬 Adding comment...")
        response = requests.post(url, headers=comment_headers, json=comment_data, timeout=30)
        
        if response.status_code == 200:
            comment_response = response.json()
            comment_id = comment_response.get('id', 'Unknown')
            print(f"   ✅ Comment added successfully! Comment ID: {comment_id}")
            return True, None
        else:
            error_msg = f"Comment error: {response.status_code} - {response.text}"
            print(f"   ❌ {error_msg}")
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Comment exception: {str(e)}"
        print(f"   ❌ {error_msg}")
        return False, error_msg

def main():
    # Test API connectivity first
    if not test_api_connectivity():
        print("❌ Cannot proceed without API connectivity. Please check:")
        print("   1. Internet connection")
        print("   2. API token validity")
        print("   3. Firewall/proxy settings")
        print("   4. Try running from a different network")
        return
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE_PATH):
        print(f"❌ Input file not found: {INPUT_FILE_PATH}")
        return
    
    print(f"✅ Found input file: {INPUT_FILE_PATH}")
    
    # Check if attachments base path exists
    if not os.path.exists(ATTACHMENTS_BASE_PATH):
        print(f"⚠️  Attachments base path not found: {ATTACHMENTS_BASE_PATH}")
        print("Continuing without attachments...")
    else:
        print(f"✅ Found attachments base path: {ATTACHMENTS_BASE_PATH}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
    
    # Read the Excel file
    print("📖 Reading Excel file...")
    try:
        df = pd.read_excel(INPUT_FILE_PATH)
    except Exception as e:
        print(f"❌ Error reading Excel file: {str(e)}")
        return
    
    # Display the columns found
    print(f"📋 Columns found in Excel file: {list(df.columns)}")
    print(f"📊 Number of rows: {len(df)}")
    
    # Check for required columns
    required_columns = ['list_id', 'name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"❌ Missing required columns: {missing_columns}")
        return
    
    # Add new columns for tracking
    df['clickuptaskid'] = None
    df['api_status'] = None
    df['api_error'] = None
    df['comment_status'] = None
    df['comment_error'] = None
    df['attachments_found'] = None
    df['attachments_uploaded'] = None
    df['attachment_errors'] = None
    
    print(f"\n🚀 Starting to process {len(df)} tasks...")
    
    # Process each row
    for index, row in df.iterrows():
        print(f"\n{'='*60}")
        print(f"📝 Processing task {index + 1}/{len(df)}")
        print(f"   Task Name: {row['name']}")
        print(f"   List ID: {row['list_id']}")
        print(f"   Original Task ID: {row['task ID']}")
        
        # Find attachments for this task
        task_id = str(row['task ID']) if pd.notna(row['task ID']) else None
        attachments = []
        if task_id and os.path.exists(ATTACHMENTS_BASE_PATH):
            attachments = find_attachments_for_task(task_id)
            df.at[index, 'attachments_found'] = len(attachments)
            if attachments:
                print(f"📎 Found {len(attachments)} attachment(s) for task {task_id}:")
                for att in attachments:
                    print(f"   - {os.path.basename(att)}")
            else:
                print(f"📎 No attachments found for task {task_id}")
        
        # Create task in ClickUp
        task_response, error = create_clickup_task(row)
        
        if task_response:
            # Extract the task ID from response
            clickup_task_id = task_response.get('id')
            df.at[index, 'clickuptaskid'] = clickup_task_id
            df.at[index, 'api_status'] = 'Success'
            print(f"✅ Task created successfully!")
            print(f"   ClickUp Task ID: {clickup_task_id}")
            print(f"   Task URL: https://app.clickup.com/t/{clickup_task_id}")
            
            # Add comment if exists
            if 'comments' in df.columns and pd.notna(row['comments']) and row['comments'] != '':
                comment_success, comment_error = add_comment_to_task(clickup_task_id, row['comments'])
                if comment_success:
                    df.at[index, 'comment_status'] = 'Success'
                else:
                    df.at[index, 'comment_status'] = 'Failed'
                    df.at[index, 'comment_error'] = comment_error
            else:
                df.at[index, 'comment_status'] = 'No Comment'
            
            # Upload attachments if any found
            uploaded_count = 0
            attachment_errors = []
            
            if attachments:
                print(f"📎 Uploading {len(attachments)} attachment(s)...")
                for attachment_path in attachments:
                    success, result = upload_attachment_to_clickup(clickup_task_id, attachment_path)
                    if success:
                        uploaded_count += 1
                    else:
                        attachment_errors.append(f"{os.path.basename(attachment_path)}: {result}")
                    
                    # Small delay between uploads
                    time.sleep(1)
            
            df.at[index, 'attachments_uploaded'] = uploaded_count
            df.at[index, 'attachment_errors'] = '; '.join(attachment_errors) if attachment_errors else None
            
            if uploaded_count > 0:
                print(f"   ✅ Successfully uploaded {uploaded_count} attachment(s)")
            if attachment_errors:
                print(f"   ❌ Failed to upload {len(attachment_errors)} attachment(s)")
                
        else:
            df.at[index, 'api_status'] = 'Failed'
            df.at[index, 'api_error'] = error
            df.at[index, 'attachments_uploaded'] = 0
            print(f"❌ Failed to create task: {error}")
        
        # Add delay to avoid rate limiting
        if index < len(df) - 1:  # Don't wait after the last task
            print("⏳ Waiting 3 seconds before next task...")
            time.sleep(3)
    
    # Save the results to Excel
    print(f"\n{'='*60}")
    print(f"💾 Saving results to {OUTPUT_FILE_PATH}...")
    try:
        df.to_excel(OUTPUT_FILE_PATH, index=False)
        print(f"✅ Results saved successfully!")
    except Exception as e:
        print(f"❌ Error saving results: {str(e)}")
        return
    
    # Print summary
    successful_tasks = len(df[df['api_status'] == 'Success'])
    failed_tasks = len(df[df['api_status'] == 'Failed'])
    total_attachments_found = df['attachments_found'].sum() if 'attachments_found' in df.columns else 0
    total_attachments_uploaded = df['attachments_uploaded'].sum() if 'attachments_uploaded' in df.columns else 0
    
    print("\n" + "="*60)
    print("📊 FINAL SUMMARY")
    print("="*60)
    print(f"📋 Total tasks processed: {len(df)}")
    print(f"✅ Successfully created tasks: {successful_tasks}")
    print(f"❌ Failed to create tasks: {failed_tasks}")
    print(f"📎 Total attachments found: {int(total_attachments_found) if total_attachments_found else 0}")
    print(f"📎 Total attachments uploaded: {int(total_attachments_uploaded) if total_attachments_uploaded else 0}")
    print(f"💾 Results saved to: {OUTPUT_FILE_PATH}")
    
    if successful_tasks > 0:
        print(f"\n🎉 {successful_tasks} tasks were successfully created in ClickUp!")
        
    if failed_tasks > 0:
        print(f"\n⚠️  Failed tasks:")
        failed_df = df[df['api_status'] == 'Failed'][['name', 'api_error']]
        for idx, failed_row in failed_df.iterrows():
            print(f"   • {failed_row['name']}: {failed_row['api_error']}")

if __name__ == "__main__":
    print("🚀 Starting ClickUp Task Import Process...")
    print(f"📁 Input file: {INPUT_FILE_PATH}")
    print(f"📁 Output file: {OUTPUT_FILE_PATH}")
    print(f"📁 Attachments path: {ATTACHMENTS_BASE_PATH}")
    print("-" * 60)
    main()
    print("\n🏁 Process completed!")