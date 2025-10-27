import pandas as pd
import requests
import json
from datetime import datetime
import time
import os
import glob
import re
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

# Global mapping to track original task ID to ClickUp task ID
task_id_mapping = {}

def test_api_connectivity():
    """Test if we can connect to ClickUp API"""
    print("🔍 Testing API connectivity...")
    try:
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

def find_existing_parent_task(original_parent_id, list_id):
    """Find existing ClickUp task by searching for the original task ID"""
    try:
        print(f"   🔍 Searching for existing parent task with original ID: {original_parent_id}")
        
        # Search for tasks in the list
        url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
        params = {
            'page': 0,
            'order_by': 'created',
            'reverse': True,
            'subtasks': True,
            'include_closed': True,
            'limit': 100
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            tasks_data = response.json()
            tasks = tasks_data.get('tasks', [])
            
            print(f"   📋 Searching through {len(tasks)} existing tasks...")
            
            # Look for task with original ID in name, description, or custom fields
            for task in tasks:
                task_name = task.get('name', '').lower()
                task_desc = task.get('description', '').lower()
                task_id_clickup = task.get('id')
                
                # Check if original parent ID is mentioned anywhere
                original_id_str = str(original_parent_id).lower()
                
                if (original_id_str in task_name or 
                    original_id_str in task_desc or
                    task_name.startswith(original_id_str) or
                    f"id: {original_id_str}" in task_desc or
                    f"#{original_id_str}" in task_name):
                    
                    print(f"   ✅ Found existing parent task: {task_id_clickup}")
                    print(f"      Task name: {task.get('name')}")
                    return task_id_clickup
            
            print(f"   ⚠️  No existing parent task found for original ID: {original_parent_id}")
            return None
        else:
            print(f"   ❌ Error searching for parent task: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   ❌ Exception searching for parent: {str(e)}")
        return None

def get_parent_clickup_id_enhanced(parent_id_excel, list_id):
    """Enhanced parent ID lookup with existing task search"""
    if pd.isna(parent_id_excel) or parent_id_excel == '':
        return None
    
    parent_id_str = str(int(parent_id_excel)) if isinstance(parent_id_excel, float) else str(parent_id_excel)
    
    # First check if parent was created in current batch
    clickup_parent_id = task_id_mapping.get(parent_id_str)
    
    if clickup_parent_id:
        print(f"   🔗 Found parent ClickUp ID from current batch: {clickup_parent_id}")
        return clickup_parent_id
    else:
        print(f"   🔍 Parent ID {parent_id_str} not in current batch, searching existing tasks...")
        # Search for existing parent task in ClickUp
        existing_parent_id = find_existing_parent_task(parent_id_str, list_id)
        if existing_parent_id:
            # Cache it for future use
            task_id_mapping[parent_id_str] = existing_parent_id
            print(f"   ✅ Cached parent mapping: {parent_id_str} -> {existing_parent_id}")
            return existing_parent_id
        else:
            print(f"   ⚠️  Creating as root task - parent {parent_id_str} not found")
            return None

def update_task_parent(task_id, parent_id):
    """Update a task's parent relationship"""
    try:
        url = f"https://api.clickup.com/api/v2/task/{task_id}"
        
        update_data = {
            "parent": parent_id
        }
        
        print(f"   🔄 Updating task {task_id} with parent {parent_id}")
        response = requests.put(url, headers=headers, json=update_data, timeout=30)
        
        if response.status_code == 200:
            print(f"   ✅ Parent relationship updated successfully")
            return True
        else:
            print(f"   ❌ Failed to update parent: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Exception updating parent: {str(e)}")
        return False

def update_parent_relationships_post_import(df_sorted):
    """Update parent relationships after all tasks are created"""
    print("\n🔄 Post-import: Checking and updating parent relationships...")
    
    parent_updates = 0
    tasks_needing_update = df_sorted[
        (df_sorted['api_status'] == 'Success') & 
        (df_sorted['parent_mapped'] == 'No') &
        df_sorted['parent ID'].notna()
    ]
    
    if len(tasks_needing_update) == 0:
        print("✅ No parent relationships need updating")
        return 0
    
    print(f"📋 Found {len(tasks_needing_update)} tasks that need parent updates")
    
    for index, row in tasks_needing_update.iterrows():
        clickup_task_id = row['clickuptaskid']
        parent_id_excel = str(int(row['parent ID'])) if isinstance(row['parent ID'], float) else str(row['parent ID'])
        list_id = str(row['list_id'])
        task_name = row['name']
        
        print(f"\n   📝 Updating parent for: {task_name}")
        print(f"      Task ID: {clickup_task_id}")
        print(f"      Looking for parent: {parent_id_excel}")
        
        # Try to find parent again (maybe it was created later in the batch)
        parent_clickup_id = task_id_mapping.get(parent_id_excel)
        
        if not parent_clickup_id:
            parent_clickup_id = find_existing_parent_task(parent_id_excel, list_id)
            if parent_clickup_id:
                task_id_mapping[parent_id_excel] = parent_clickup_id
        
        if parent_clickup_id:
            # Update the task with parent relationship
            success = update_task_parent(clickup_task_id, parent_clickup_id)
            if success:
                df_sorted.at[index, 'parent_mapped'] = 'Updated Post-Import'
                parent_updates += 1
        else:
            print(f"      ❌ Parent task still not found")
            df_sorted.at[index, 'parent_mapped'] = 'Parent Not Found'
    
    print(f"\n🔗 Updated {parent_updates} parent relationships post-import")
    return parent_updates

def create_custom_fields_comment(row):
    """Create a formatted comment with custom field information"""
    custom_info = []
    
    if pd.notna(row.get('Billable')) and str(row['Billable']).strip():
        custom_info.append(f"💰 **Billable:** {row['Billable']}")
    
    if pd.notna(row.get('Delayed')) and str(row['Delayed']).strip():
        custom_info.append(f"⏰ **Delayed:** {row['Delayed']}")
    
    if pd.notna(row.get('former assignee')) and str(row['former assignee']).strip():
        custom_info.append(f"👤 **Former Assignee:** {row['former assignee']}")
    
    if pd.notna(row.get('Version')) and str(row['Version']).strip():
        custom_info.append(f"🏷️ **Version:** {row['Version']}")
    
    if custom_info:
        header = "📋 **Custom Field Information:**\n"
        return header + "\n".join(custom_info)
    
    return None

def track_time_on_task(clickup_task_id, spent_time_hours):
    """Add time tracking entry to a ClickUp task"""
    if pd.isna(spent_time_hours) or spent_time_hours == '' or float(spent_time_hours) <= 0:
        return True, None
    
    try:
        spent_milliseconds = int(float(spent_time_hours) * 3600000)  # Convert hours to milliseconds
        
        url = f"https://api.clickup.com/api/v2/task/{clickup_task_id}/time"
        
        # Get current timestamp for the time entry
        current_timestamp = int(time.time() * 1000)
        start_timestamp = current_timestamp - spent_milliseconds
        
        time_data = {
            "description": "Time imported from Excel",
            "start": start_timestamp,
            "end": current_timestamp,
            "billable": True,
            "assignee": None  # Will use task assignee
        }
        
        time_headers = {
            "accept": "application/json",
            "content-type": "application/json", 
            "Authorization": CLICKUP_API_TOKEN
        }
        
        print(f"   ⏱️  Adding {spent_time_hours} hours of tracked time...")
        response = requests.post(url, headers=time_headers, json=time_data, timeout=30)
        
        if response.status_code == 200:
            time_response = response.json()
            time_entry_id = time_response.get('id', 'Unknown')
            print(f"   ✅ Time tracking added successfully! Entry ID: {time_entry_id}")
            return True, None
        else:
            error_msg = f"Time tracking error: {response.status_code} - {response.text}"
            print(f"   ❌ {error_msg}")
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Time tracking exception: {str(e)}"
        print(f"   ❌ {error_msg}")
        return False, error_msg

def sort_tasks_by_hierarchy(df):
    """Sort tasks so parent tasks are processed before child tasks"""
    processed_tasks = []
    remaining_tasks = df.copy()
    
    while len(remaining_tasks) > 0:
        processed_parent_ids = set(str(task_id) for task_id in processed_tasks)
        
        processable_tasks = []
        for index, row in remaining_tasks.iterrows():
            parent_id = str(int(row['parent ID'])) if pd.notna(row['parent ID']) and isinstance(row['parent ID'], float) else str(row['parent ID']) if pd.notna(row['parent ID']) else None
            
            if parent_id is None or parent_id == '' or parent_id == 'nan' or parent_id in processed_parent_ids:
                processable_tasks.append(index)
        
        if not processable_tasks:
            print("⚠️  Possible circular dependency detected in parent-child relationships")
            processable_tasks = remaining_tasks.index.tolist()
        
        for index in processable_tasks:
            row = remaining_tasks.loc[index]
            task_id_val = str(int(row['task ID'])) if isinstance(row['task ID'], float) else str(row['task ID'])
            processed_tasks.append(task_id_val)
        
        remaining_tasks = remaining_tasks.drop(processable_tasks)
    
    task_id_to_original_index = {}
    for idx, row in df.iterrows():
        task_id_val = str(int(row['task ID'])) if isinstance(row['task ID'], float) else str(row['task ID'])
        task_id_to_original_index[task_id_val] = idx
    
    ordered_indices = [task_id_to_original_index[task_id] for task_id in processed_tasks]
    
    return df.loc[ordered_indices].reset_index(drop=True)

def find_attachments_for_task(task_id):
    """Find all attachments for a given task ID"""
    task_folder = os.path.join(ATTACHMENTS_BASE_PATH, str(task_id))
    attachments = []
    
    if os.path.exists(task_folder):
        file_patterns = ['*.png', '*.jpg', '*.jpeg', '*.gif', '*.bmp', '*.webp', '*.pdf', '*.doc', '*.docx', '*.*']
        for pattern in file_patterns:
            files = glob.glob(os.path.join(task_folder, pattern))
            attachments.extend(files)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_attachments = []
        for file_path in attachments:
            if file_path not in seen:
                seen.add(file_path)
                unique_attachments.append(file_path)
        attachments = unique_attachments
    
    return attachments

def upload_single_file_advanced(clickup_task_id, file_path):
    """Advanced file upload with better content type detection and error handling"""
    try:
        if not os.path.exists(file_path):
            return False, f"File not found: {file_path}"
        
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        print(f"   📎 Uploading: {file_name} ({file_size} bytes)")
        
        with open(file_path, 'rb') as file:
            file_content = file.read()
        
        _, ext = os.path.splitext(file_path.lower())
        content_type_map = {
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
            '.webp': 'image/webp',
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        }
        content_type = content_type_map.get(ext, 'application/octet-stream')
        
        files = {
            'attachment': (file_name, file_content, content_type)
        }
        
        headers_upload = {"Authorization": CLICKUP_API_TOKEN}
        upload_url = f"https://api.clickup.com/api/v2/task/{clickup_task_id}/attachment"
        
        response = requests.post(upload_url, files=files, headers=headers_upload, timeout=120)
        
        if response.status_code == 200:
            attachment_data = response.json()
            attachment_id = attachment_data.get('id', 'Unknown')
            attachment_url = attachment_data.get('url', '')
            print(f"   ✅ Uploaded successfully! ID: {attachment_id}")
            return True, {
                'id': attachment_id,
                'url': attachment_url,
                'filename': file_name
            }
        else:
            error_msg = f"Upload failed: {response.status_code} - {response.text}"
            print(f"   ❌ {error_msg}")
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Upload exception: {str(e)}"
        print(f"   ❌ {error_msg}")
        return False, error_msg

def upload_all_attachments_advanced(clickup_task_id, attachments, original_description=""):
    """Advanced attachment upload with description integration"""
    uploaded_count = 0
    attachment_errors = []
    uploaded_files = []
    
    if not attachments:
        return uploaded_count, attachment_errors
    
    print(f"📎 Starting advanced upload of {len(attachments)} attachment(s)...")
    
    for attachment_path in attachments:
        success, result = upload_single_file_advanced(clickup_task_id, attachment_path)
        if success:
            uploaded_count += 1
            uploaded_files.append(result)
        else:
            attachment_errors.append(f"{os.path.basename(attachment_path)}: {result}")
        
        time.sleep(1)
    
    return uploaded_count, attachment_errors

def create_clickup_task(row):
    """Create a task in ClickUp without custom fields"""
    list_id = str(row['list_id'])
    
    # Get parent ClickUp ID if parent exists (enhanced version)
    parent_clickup_id = get_parent_clickup_id_enhanced(row.get('parent ID'), list_id)
    
    # Prepare the task data WITHOUT custom fields
    task_data = {
        "name": str(row['name']),
        "description": str(row['description']) if pd.notna(row['description']) else "",
        "assignees": [int(row['assignee ID'])] if pd.notna(row['assignee ID']) else [],
        "status": get_status_id(str(row['status']), list_id) if pd.notna(row['status']) else None,
        "priority": get_priority_value(row['priority']),
        "due_date": convert_date_to_timestamp(row['due_date']),
        "time_estimate": int(float(row['time_estimate']) * 3600000) if pd.notna(row['time_estimate']) else None,
        "start_date": convert_date_to_timestamp(row['start_date_time']),
        "parent": parent_clickup_id,
        "notify_all": False,
        "check_required_custom_fields": False
    }
    
    # Remove None values
    task_data = {k: v for k, v in task_data.items() if v is not None}
    
    # API endpoint
    url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
    
    try:
        print(f"🔄 Creating task {'with parent' if parent_clickup_id else 'as root task'}...")
        if parent_clickup_id:
            print(f"   🔗 Parent ClickUp ID: {parent_clickup_id}")
        
        response = requests.post(url, headers=headers, json=task_data, timeout=60)
        
        if response.status_code == 200:
            task_response = response.json()
            clickup_task_id = task_response.get('id')
            
            # Store mapping
            original_task_id = str(int(row['task ID'])) if isinstance(row['task ID'], float) else str(row['task ID'])
            task_id_mapping[original_task_id] = clickup_task_id
            
            print(f"✅ Task created successfully!")
            print(f"   Mapping: {original_task_id} -> {clickup_task_id}")
            return task_response, None, bool(parent_clickup_id)
        else:
            error_msg = f"Error creating task: {response.status_code} - {response.text}"
            print(f"❌ {error_msg}")
            return None, error_msg, False
            
    except Exception as e:
        error_msg = f"Exception creating task: {str(e)}"
        print(f"❌ {error_msg}")
        return None, error_msg, False

def add_comment_with_custom_fields(clickup_task_id, comment_text, custom_field_comment):
    """Add a comment with custom field information and original comment"""
    combined_comment = ""
    
    if custom_field_comment:
        combined_comment = custom_field_comment
    
    if comment_text and pd.notna(comment_text) and str(comment_text).strip():
        if combined_comment:
            combined_comment += f"\n\n📝 **Original Comment:**\n{comment_text}"
        else:
            combined_comment = str(comment_text)
    
    if not combined_comment:
        return True, None
    
    url = f"https://api.clickup.com/api/v2/task/{clickup_task_id}/comment"
    
    comment_data = {
        "comment_text": combined_comment,
        "assignee": None,
        "notify_all": False
    }
    
    comment_headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": CLICKUP_API_TOKEN
    }
    
    try:
        print(f"   💬 Adding comment with custom field data...")
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
    
    # Sort tasks by hierarchy (parents first)
    print("🔄 Sorting tasks by parent-child hierarchy...")
    df_sorted = sort_tasks_by_hierarchy(df)
    print("✅ Tasks sorted for proper parent-child creation order")
    
    # Add new columns for tracking
    df_sorted['clickuptaskid'] = None
    df_sorted['api_status'] = None
    df_sorted['api_error'] = None
    df_sorted['comment_status'] = None
    df_sorted['comment_error'] = None
    df_sorted['attachments_found'] = None
    df_sorted['attachments_uploaded'] = None
    df_sorted['attachment_errors'] = None
    df_sorted['time_tracking_status'] = None
    df_sorted['custom_fields_in_comment'] = None
    df_sorted['parent_mapped'] = None
    
    print(f"\n🚀 Starting to process {len(df_sorted)} tasks...")
    print("ℹ️  Custom fields will be added as comments to avoid plan limits")
    print("🔗 Enhanced parent-child relationship handling enabled")
    
    # Process each row
    for index, row in df_sorted.iterrows():
        print(f"\n{'='*60}")
        print(f"📝 Processing task {index + 1}/{len(df_sorted)}")
        print(f"   Task Name: {row['name']}")
        print(f"   List ID: {row['list_id']}")
        
        original_task_id = str(int(row['task ID'])) if isinstance(row['task ID'], float) else str(row['task ID'])
        print(f"   Original Task ID: {original_task_id}")
        
        if pd.notna(row.get('parent ID')):
            parent_id_display = str(int(row['parent ID'])) if isinstance(row['parent ID'], float) else str(row['parent ID'])
            print(f"   Parent ID: {parent_id_display}")
        
        # Find attachments for this task
        attachments = []
        if original_task_id and os.path.exists(ATTACHMENTS_BASE_PATH):
            attachments = find_attachments_for_task(original_task_id)
            df_sorted.at[index, 'attachments_found'] = len(attachments)
            if attachments:
                print(f"📎 Found {len(attachments)} attachment(s) for task {original_task_id}")
            else:
                print(f"📎 No attachments found for task {original_task_id}")
        
        # Create task in ClickUp
        task_response, error, parent_found = create_clickup_task(row)
        
        if task_response:
            # Extract the task ID from response
            clickup_task_id = task_response.get('id')
            df_sorted.at[index, 'clickuptaskid'] = clickup_task_id
            df_sorted.at[index, 'api_status'] = 'Success'
            df_sorted.at[index, 'parent_mapped'] = 'Yes' if parent_found else ('No' if pd.notna(row.get('parent ID')) else 'N/A')
            
            print(f"✅ Task created successfully!")
            print(f"   ClickUp Task ID: {clickup_task_id}")
            print(f"   Task URL: https://app.clickup.com/t/{clickup_task_id}")
            
            # Add time tracking if spent time exists
            if 'spent time' in row and pd.notna(row['spent time']):
                time_success, time_error = track_time_on_task(clickup_task_id, row['spent time'])
                if time_success:
                    df_sorted.at[index, 'time_tracking_status'] = 'Success'
                else:
                    df_sorted.at[index, 'time_tracking_status'] = f'Failed: {time_error}'
            else:
                df_sorted.at[index, 'time_tracking_status'] = 'No Time Data'
            
            # Create custom fields comment
            custom_field_comment = create_custom_fields_comment(row)
            has_custom_data = custom_field_comment is not None
            
            # Add comment with custom field data and original comment
            comment_text = row.get('comments', '')
            if custom_field_comment or (pd.notna(comment_text) and str(comment_text).strip()):
                comment_success, comment_error = add_comment_with_custom_fields(
                    clickup_task_id, comment_text, custom_field_comment
                )
                if comment_success:
                    df_sorted.at[index, 'comment_status'] = 'Success'
                    df_sorted.at[index, 'custom_fields_in_comment'] = 'Yes' if has_custom_data else 'No'
                else:
                    df_sorted.at[index, 'comment_status'] = 'Failed'
                    df_sorted.at[index, 'comment_error'] = comment_error
                    df_sorted.at[index, 'custom_fields_in_comment'] = 'Failed'
            else:
                df_sorted.at[index, 'comment_status'] = 'No Comment'
                df_sorted.at[index, 'custom_fields_in_comment'] = 'No Data'
            
            # Upload attachments using advanced method
            uploaded_count = 0
            attachment_errors = []
            
            if attachments:
                original_description = str(row['description']) if pd.notna(row['description']) else ""
                uploaded_count, attachment_errors = upload_all_attachments_advanced(
                    clickup_task_id, attachments, original_description
                )
            
            df_sorted.at[index, 'attachments_uploaded'] = uploaded_count
            df_sorted.at[index, 'attachment_errors'] = '; '.join(attachment_errors) if attachment_errors else None
            
            if uploaded_count > 0:
                print(f"   ✅ Successfully uploaded {uploaded_count} attachment(s)")
            if attachment_errors:
                print(f"   ❌ Failed to upload {len(attachment_errors)} attachment(s)")
                
        else:
            df_sorted.at[index, 'api_status'] = 'Failed'
            df_sorted.at[index, 'api_error'] = error
            df_sorted.at[index, 'attachments_uploaded'] = 0
            df_sorted.at[index, 'time_tracking_status'] = 'Task Creation Failed'
            df_sorted.at[index, 'parent_mapped'] = 'Task Creation Failed'
            df_sorted.at[index, 'custom_fields_in_comment'] = 'Task Creation Failed'
            print(f"❌ Failed to create task: {error}")
        
        # Add delay to avoid rate limiting
        if index < len(df_sorted) - 1:
            print("⏳ Waiting 3 seconds before next task...")
            time.sleep(3)
    
    # Update any missing parent relationships
    parent_updates = update_parent_relationships_post_import(df_sorted)
    
    # Save the results to Excel
    print(f"\n{'='*60}")
    print(f"💾 Saving results to {OUTPUT_FILE_PATH}...")
    try:
        df_sorted.to_excel(OUTPUT_FILE_PATH, index=False)
        print(f"✅ Results saved successfully!")
    except Exception as e:
        print(f"❌ Error saving results: {str(e)}")
        return
    
    # Print summary
    successful_tasks = len(df_sorted[df_sorted['api_status'] == 'Success'])
    failed_tasks = len(df_sorted[df_sorted['api_status'] == 'Failed'])
    total_attachments_found = df_sorted['attachments_found'].sum() if 'attachments_found' in df_sorted.columns else 0
    total_attachments_uploaded = df_sorted['attachments_uploaded'].sum() if 'attachments_uploaded' in df_sorted.columns else 0
    time_tracking_success = len(df_sorted[df_sorted['time_tracking_status'] == 'Success'])
    parent_tasks_mapped = len(df_sorted[df_sorted['parent_mapped'] == 'Yes']) + len(df_sorted[df_sorted['parent_mapped'] == 'Updated Post-Import'])
    custom_fields_in_comments = len(df_sorted[df_sorted['custom_fields_in_comment'] == 'Yes'])
    
    print("\n" + "="*60)
    print("📊 FINAL SUMMARY")
    print("="*60)
    print(f"📋 Total tasks processed: {len(df_sorted)}")
    print(f"✅ Successfully created tasks: {successful_tasks}")
    print(f"❌ Failed to create tasks: {failed_tasks}")
    print(f"📎 Total attachments found: {int(total_attachments_found) if total_attachments_found else 0}")
    print(f"📎 Total attachments uploaded: {int(total_attachments_uploaded) if total_attachments_uploaded else 0}")
    print(f"⏱️  Time tracking entries added: {time_tracking_success}")
    print(f"🔗 Parent-child relationships mapped: {parent_tasks_mapped}")
    print(f"📝 Custom field data added as comments: {custom_fields_in_comments}")
    print(f"🔄 Post-import parent updates: {parent_updates}")
    print(f"💾 Results saved to: {OUTPUT_FILE_PATH}")
    
    # Print task ID mapping summary
    if task_id_mapping:
        print(f"\n📋 Task ID Mapping Summary:")
        print("-" * 40)
        for original_id, clickup_id in list(task_id_mapping.items())[:5]:  # Show first 5
            print(f"   {original_id} -> {clickup_id}")
        if len(task_id_mapping) > 5:
            print(f"   ... and {len(task_id_mapping) - 5} more")
    
    if successful_tasks > 0:
        print(f"\n🎉 {successful_tasks} tasks were successfully created in ClickUp!")
        print("🔗 Check the task URLs in the output Excel file to view your tasks")
        
        if parent_tasks_mapped > 0:
            print(f"🔗 {parent_tasks_mapped} parent-child relationships were successfully mapped!")
            
        if custom_fields_in_comments > 0:
            print(f"📝 {custom_fields_in_comments} tasks had custom field data added as comments!")
            
        if time_tracking_success > 0:
            print(f"⏱️  {time_tracking_success} tasks had time tracking entries added!")
        
        if parent_updates > 0:
            print(f"🔄 {parent_updates} parent relationships were updated post-import!")
        
    if failed_tasks > 0:
        print(f"\n⚠️  Failed tasks:")
        failed_df = df_sorted[df_sorted['api_status'] == 'Failed'][['name', 'api_error']]
        for idx, failed_row in failed_df.iterrows():
            print(f"   • {failed_row['name']}: {failed_row['api_error']}")

if __name__ == "__main__":
    print("🚀 Starting Enhanced ClickUp Task Import Process...")
    print(f"📁 Input file: {INPUT_FILE_PATH}")
    print(f"📁 Output file: {OUTPUT_FILE_PATH}")
    print(f"📁 Attachments path: {ATTACHMENTS_BASE_PATH}")
    print("🔧 Enhanced Features:")
    print("   • ⏱️  Time tracking support")
    print("   • 🔗 Enhanced parent-child task relationships with existing task search")
    print("   • 📝 Custom fields as comments (avoids plan limits)")
    print("   • 📎 Advanced attachment handling")
    print("   • 📋 HTML description parsing")
    print("   • 🔄 Post-import parent relationship updates")
    print("-" * 60)
    main()
    print("\n🏁 Enhanced import process completed!")
    print("\n📋 Output file columns include:")
    print("   • clickuptaskid - Generated ClickUp task ID")  
    print("   • time_tracking_status - Time tracking success/failure")
    print("   • parent_mapped - Parent relationship mapping status")
    print("   • custom_fields_in_comment - Custom fields added to comments")
    print("   • attachment_errors - Details of any attachment upload failures")
    print("   • All original Excel columns preserved")