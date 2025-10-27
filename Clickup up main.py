import pandas as pd
import requests
import json
from datetime import datetime
import time
import os

# Configuration
INPUT_FILE_PATH = r"E:\clickup attachments\Input\PM_App_Export_September-23-2025.xlsx"
OUTPUT_FILE_PATH = r"E:\clickup attachments\dataoutput\clickupoutput.xlsx"
CLICKUP_API_TOKEN = "pk_100804363_VPAQ8T8R1OCVFPTHTXI3PJLIO4MZB4RP"

# ClickUp API headers
headers = {
    "Authorization": CLICKUP_API_TOKEN,
    "Content-Type": "application/json"
}

def convert_date_to_timestamp(date_str):
    """Convert date string to Unix timestamp in milliseconds"""
    if pd.isna(date_str) or date_str == '':
        return None
    try:
        # Try different date formats
        formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                dt = datetime.strptime(str(date_str), fmt)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
        # If no format matches, try pandas to_datetime
        dt = pd.to_datetime(date_str)
        return int(dt.timestamp() * 1000)
    except:
        return None

def get_status_id(status_name, list_id):
    """Get status ID from status name"""
    # Common ClickUp statuses - adjust based on your workspace setup
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
        "time_estimate": int(float(row['time_estimate']) * 3600000) if pd.notna(row['time_estimate']) else None,  # Convert hours to milliseconds
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
        # Make the API request
        print(f"Creating task with data: {json.dumps(task_data, indent=2)}")
        response = requests.post(url, headers=headers, json=task_data)
        
        if response.status_code == 200:
            return response.json(), None
        else:
            error_msg = f"Error creating task: {response.status_code} - {response.text}"
            print(error_msg)
            return None, error_msg
            
    except Exception as e:
        error_msg = f"Exception creating task: {str(e)}"
        print(error_msg)
        return None, error_msg

def add_comment_to_task(clickup_task_id, comment_text):
    """Add a comment to the created task using ClickUp task ID"""
    if pd.isna(comment_text) or comment_text == '':
        return True, None
    
    # Use POST method to create a comment (not GET)
    url = f"https://api.clickup.com/api/v2/task/{clickup_task_id}/comment"
    
    # Prepare comment data
    comment_data = {
        "comment_text": str(comment_text),
        "assignee": None,  # Optional: assign comment to someone
        "notify_all": False  # Set to True if you want to notify all task members
    }
    
    # Headers for comment creation
    comment_headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": CLICKUP_API_TOKEN
    }
    
    try:
        print(f"   Adding comment to task {clickup_task_id}: {comment_text[:50]}...")
        response = requests.post(url, headers=comment_headers, json=comment_data)
        
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
    # Check if input file exists
    if not os.path.exists(INPUT_FILE_PATH):
        print(f"❌ Input file not found: {INPUT_FILE_PATH}")
        print("Please make sure the file exists and the path is correct.")
        return
    
    print(f"✅ Found input file: {INPUT_FILE_PATH}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
    
    # Read the Excel file
    print("Reading Excel file...")
    try:
        df = pd.read_excel(INPUT_FILE_PATH)
    except Exception as e:
        print(f"❌ Error reading Excel file: {str(e)}")
        return
    
    # Display the columns found
    print(f"Columns found in Excel file: {list(df.columns)}")
    print(f"Number of rows: {len(df)}")
    
    # Check for required columns
    required_columns = ['list_id', 'name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"❌ Missing required columns: {missing_columns}")
        return
    
    # Add new columns for ClickUp task ID and status
    df['clickuptaskid'] = None
    df['api_status'] = None
    df['api_error'] = None
    df['comment_status'] = None
    df['comment_error'] = None
    
    print(f"Found {len(df)} tasks to process...")
    
    # Process each row
    for index, row in df.iterrows():
        print(f"\n{'='*60}")
        print(f"Processing task {index + 1}/{len(df)}")
        print(f"Task Name: {row['name']}")
        print(f"List ID: {row['list_id']}")
        
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
            
            # Add comment if exists (using the ClickUp task ID)
            if 'comments' in df.columns and pd.notna(row['comments']) and row['comments'] != '':
                comment_success, comment_error = add_comment_to_task(clickup_task_id, row['comments'])
                if comment_success:
                    df.at[index, 'comment_status'] = 'Success'
                else:
                    df.at[index, 'comment_status'] = 'Failed'
                    df.at[index, 'comment_error'] = comment_error
            else:
                df.at[index, 'comment_status'] = 'No Comment'
                
        else:
            df.at[index, 'api_status'] = 'Failed'
            df.at[index, 'api_error'] = error
            print(f"❌ Failed to create task: {error}")
        
        # Add a small delay to avoid rate limiting
        time.sleep(1.5)  # Slightly longer delay for API rate limits
    
    # Save the results to Excel
    print(f"\n{'='*60}")
    print(f"Saving results to {OUTPUT_FILE_PATH}...")
    try:
        df.to_excel(OUTPUT_FILE_PATH, index=False)
        print(f"✅ Results saved successfully!")
    except Exception as e:
        print(f"❌ Error saving results: {str(e)}")
        return
    
    # Print summary
    successful_tasks = len(df[df['api_status'] == 'Success'])
    failed_tasks = len(df[df['api_status'] == 'Failed'])
    successful_comments = len(df[df['comment_status'] == 'Success'])
    failed_comments = len(df[df['comment_status'] == 'Failed'])
    
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"📊 Total tasks processed: {len(df)}")
    print(f"✅ Successfully created tasks: {successful_tasks}")
    print(f"❌ Failed to create tasks: {failed_tasks}")
    print(f"💬 Successfully added comments: {successful_comments}")
    print(f"⚠️  Failed to add comments: {failed_comments}")
    print(f"💾 Results saved to: {OUTPUT_FILE_PATH}")
    
    if successful_tasks > 0:
        print(f"\n🎉 {successful_tasks} tasks were successfully created in ClickUp!")
        
    if failed_tasks > 0:
        print(f"\n⚠️  Failed tasks:")
        failed_df = df[df['api_status'] == 'Failed'][['name', 'api_error']]
        for idx, failed_row in failed_df.iterrows():
            print(f"   • {failed_row['name']}: {failed_row['api_error']}")
    
    if failed_comments > 0:
        print(f"\n⚠️  Failed comments:")
        failed_comments_df = df[df['comment_status'] == 'Failed'][['name', 'comment_error']]
        for idx, failed_row in failed_comments_df.iterrows():
            print(f"   • {failed_row['name']}: {failed_row['comment_error']}")

if __name__ == "__main__":
    print("🚀 Starting ClickUp Task Import Process...")
    print(f"📁 Input file: {INPUT_FILE_PATH}")
    print(f"📁 Output file: {OUTPUT_FILE_PATH}")
    print("-" * 60)
    main()
    print("\n🏁 Process completed!")