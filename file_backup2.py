import asyncio
import os
from datetime import timedelta
from typing import List, Tuple
import logging
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import CancelledError
import threading
import uuid
# from temporalio.client import Client


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def manage_workflow(handle, workflow_id):
    while True:
        action = input("Enter 'pause', 'resume', 'terminate', 'cancel' or 'q' to quit: ").lower()
        if action == 'pause':
            asyncio.run(handle.signal(FileBackupWorkflow.pause_backup))

        elif action == 'cancel':
            asyncio.run(handle.cancel())
            break

        elif action == 'resume':
            asyncio.run(handle.signal(FileBackupWorkflow.resume_backup))
            # print(f"Resume signal sent to workflow {workflow_id}")

        elif action == 'terminate':
            asyncio.run(handle.terminate(reason="User requested termination"))
            break

        elif action == 'q':
            # print("Exiting the workflow.")
            break
        else:
            print("Invalid input. Please try again.")


@activity.defn
async def skip_task(source_folder: str):
    pass
    # activity.logger.info(f"No files to update in source folder: {source_folder}")

@activity.defn
async def list_files_activity(source_folder: str, backup_folder: str) -> List[Tuple[str, str]]:
    files_to_update = []

    # if source_folder=="/Users/aasthathorat/temporal-project/source1":
    #     raise Exception

    if not os.path.exists(source_folder):
        logger.error(f"Source folder '{source_folder}' does not exist.")
        raise Exception(f"Source folder '{source_folder}' does not exist.")

    try:
        # logger.info(f"Checking files in source folder: {source_folder}")
        for root, _, files in os.walk(source_folder):
            for file in files:
                source_file = os.path.join(root, file)
                relative_path = os.path.relpath(source_file, source_folder)
                backup_file = os.path.join(backup_folder, relative_path)

                source_mtime = os.path.getmtime(source_file)
                if os.path.exists(backup_file):
                    backup_mtime = os.path.getmtime(backup_file)
                    if source_mtime > backup_mtime:
                        files_to_update.append((source_file, backup_file))
                else:
                    files_to_update.append((source_file, backup_file))

        # logger.info(f"Found {len(files_to_update)} files to update in {source_folder}")
        return files_to_update

    except Exception as e:
        error_message = f"Error during file listing for folder '{source_folder}': {e}"
        # logger.error(error_message)
        raise




@activity.defn
async def copy_files_activity(files_to_update: List[Tuple[str, str]], source_folder: str, workflow_id: str):
    # print(f"Inside copy files for {source_folder}")
    client = await Client.connect("localhost:7233")
    files_copied = 0
    # print(source_folder)
    for source_file, backup_file in files_to_update:
        try:
            os.makedirs(os.path.dirname(backup_file), exist_ok=True)
            with open(source_file, "rb") as src, open(backup_file, "wb") as dst:
                dst.write(src.read())
            files_copied += 1
            # print(f"files copied -> {files_copied}")
            activity.heartbeat()

            handle = client.get_workflow_handle(workflow_id)

            # # Query the workflow

            is_paused = await handle.query(FileBackupWorkflow.is_paused)
            # print("is paused ",is_paused)

            if is_paused:
                return "PAUSE",files_copied

            # if files_copied == pause_after and pause_after == 50:
            #     return "PAUSE"
        except Exception as e:
            pass
            # print(e)
    return "Unpause",files_copied
       
@workflow.defn
class FileBackupWorkflow:
    def __init__(self):
        self.paused = False
        self.files_copied = {}
        self.folder_statuses = {}
        self.initial_signal_received = False
        self.direct_copy_folders = []
    
    @workflow.query
    def is_paused(self) -> bool:
        return self.paused
    
    @workflow.signal
    def initial_signal(self, data):
        print("signal received ")
        self.initial_signal_received = True
        self.direct_copy_folders = list(data.get('direct_copy_folders', []))

    @workflow.run
    async def run(self, source_folders: List[str], backup_folder: str, workflow_id: str):

        # Wait for the initial signal if it hasn't been received
        await workflow.wait_condition(lambda: self.initial_signal_received)
        
        
        # Run list_files_activity for all folders in parallel
        list_tasks = [
            workflow.execute_activity(
                list_files_activity,
                args=[folder, backup_folder],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            for folder in source_folders
        ]
        files_to_update_list = await asyncio.gather(*list_tasks, return_exceptions=True)

            # Process each folder in parallel
        process_tasks = [
            self.process_folder(folder, files_to_update, backup_folder, workflow_id)
            for folder, files_to_update in zip(source_folders, files_to_update_list)
            if not isinstance(files_to_update, Exception)
        ]
        await asyncio.gather(*process_tasks)
        print("after gathering all from list files task no exceptionsss")
        
         # Handle folders that failed during list_files_activity
        for folder, files_to_update in zip(source_folders, files_to_update_list):
            if isinstance(files_to_update, Exception):
                self.folder_statuses[folder] = {'success': False, 'error': f"List files task failed: {str(files_to_update)}"}
       

    async def process_folder(self, source_folder: str, files_to_update: List[str], backup_folder: str, workflow_id: str):
        try:
           

            self.files_copied[source_folder] = 0
            if len(files_to_update) > 0:
                copy_result = await workflow.execute_activity(
                    copy_files_activity,
                    args=[files_to_update, source_folder, workflow_id],
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(seconds=1),
                )
                
                if copy_result is None:
                    raise ValueError(f"Copy files activity for {source_folder} returned None")
                
                result, files_copied = copy_result
                self.files_copied[source_folder] = files_copied
                
                if result == "PAUSE":
                    self.paused = True
                    await workflow.wait_condition(lambda: not self.paused)

            # Copy remaining files
            if len(files_to_update) > self.files_copied[source_folder]:
                remaining_result = await workflow.execute_activity(
                    copy_files_activity,
                    args=[files_to_update[self.files_copied[source_folder]:], source_folder, workflow_id],
                    start_to_close_timeout=timedelta(minutes=30),
                    heartbeat_timeout=timedelta(seconds=30),
                )
                
                if remaining_result is not None:
                    _, additional_files_copied = remaining_result
                    self.files_copied[source_folder] += additional_files_copied

            if len(files_to_update)==0:
                # If there are no files to update, call the skip_task activity
                await workflow.execute_activity(
                    skip_task,
                    args=[source_folder],
                    start_to_close_timeout=timedelta(seconds=1),
                )
            

            print(f"\nFinished processing all {self.files_copied[source_folder]} files from {source_folder}")
            self.folder_statuses[source_folder] = {'success': True, 'error': None}
        
        except Exception as e:
            self.folder_statuses[source_folder] = {'success': False, 'error': str(e)}
            print(f"\nError processing folder {source_folder}: {str(e)}\n")

    @workflow.signal
    def pause_backup(self):
        print("\n\tReceived signal to pause backup\n")
        self.paused = True

    @workflow.signal
    def resume_backup(self):
        print("\n\tReceived signal to resume backup\n")
        self.paused = False
    

# main method
async def main():
    client = await Client.connect("localhost:7233")
    print("A connection to the Temporal server is established\n\n")
    
    source_folders = [
        "/Users/aasthathorat/temporal-project/source1",
        "/Users/aasthathorat/temporal-project/source4",
         "/Users/aasthathorat/temporal-project/source2",
        #  "/Users/aasthathorat/temporal-project/source3",
        #  "/Users/aasthathorat/temporal-project/source6"

    ]
    backup_folder = "/Users/aasthathorat/temporal-project/backup2"

     # Prepare the initial signal data
    initial_signal_data = {
        'direct_copy_folders': ['/Users/aasthathorat/temporal-project/source1']
    }

   

    worker = Worker(
        client,
        task_queue="file-backup-task-queue",
        workflows=[FileBackupWorkflow],
        activities=[list_files_activity, copy_files_activity , skip_task],
    )

    async with worker:
        workflow_id = f"file-backup-{uuid.uuid4()}"
        handle = await client.start_workflow(
            FileBackupWorkflow.run,
            args=[source_folders, backup_folder, workflow_id],
            id=workflow_id,
            task_queue="file-backup-task-queue",
            task_timeout=timedelta(seconds=30)
        )
        await handle.signal(FileBackupWorkflow.initial_signal, initial_signal_data)
        
        print(f"Workflow started with ID: {workflow_id}")


        

        # Start the termination input thread
        termination_thread = threading.Thread(target=manage_workflow, args=(handle,workflow_id))
        termination_thread.start()

        try:
            result = await handle.result()
            # print("Workflow completed successfully.")
        except Exception as e:
            pass
            # print(f"Workflow failed or was terminated: {e}")

        # Wait for the termination thread to finish
        termination_thread.join()
    
   

if __name__ == "__main__":
    asyncio.run(main())
