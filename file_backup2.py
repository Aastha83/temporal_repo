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




def cancel_workflow(handle,workflow_id):
    asyncio.run(handle.cancel())
    # print(f"Cancellation requested for workflow with ID: {workflow_id}")
       
    


def terminate_workflow(handle, workflow_id):
    asyncio.run(handle.terminate(reason="User requested termination"))
    # print("Workflow termination requested.")


def manage_workflow(handle, workflow_id):
    while True:
        action = input("Enter 'pause', 'resume', 'terminate', or 'q' to quit: ").lower()
        if action == 'pause':
            asyncio.run(handle.signal(FileBackupWorkflow.pause_backup))
            # print(f"Pause signal sent to workflow {workflow_id}")

        elif action == 'resume':
            asyncio.run(handle.signal(FileBackupWorkflow.resume_backup))
            # print(f"Resume signal sent to workflow {workflow_id}")

        elif action == 'terminate':
            asyncio.run(handle.terminate(reason="User requested termination"))
            # print(f"Termination requested for workflow {workflow_id}")
            # break

        elif action == 'q':
            print("Exiting without terminating the workflow.")
            break
        else:
            print("Invalid input. Please try again.")
    

# def manage_workflow(handle,workflow_id):
    
#     while True:
#         action = input("Enter 'terminate' to terminate \n'cancel' to cancel the worflow or 'q' to quit: ").strip().lower()
#         if action == "terminate":
#             terminate_workflow(handle, workflow_id)
#         elif action == "cancel":
#             cancel_workflow(handle, workflow_id)
#         elif action == "q":
#             break
#         else:
#             print("Invalid input. Please enter 'terminate' or 'q'.")



@activity.defn
async def skip_task(source_folder: str):
    pass
    # activity.logger.info(f"No files to update in source folder: {source_folder}")

@activity.defn
async def list_files_activity(source_folder: str, backup_folder: str) -> List[Tuple[str, str]]:
    files_to_update = []

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
    
    # return f"Copied {files_copied} files"


@workflow.defn
class FileBackupWorkflow:
    def __init__(self):
        self.paused = False
        self.files_copied = 0
        self.current_folder = None
    
    @workflow.query
    def is_paused(self) -> bool:
        return self.paused

    @workflow.run
    async def run(self, source_folders: List[str], backup_folder: str, workflow_id : str):
        print("Starting file backup workflow.")
        for folder in source_folders:
            self.current_folder = folder
            await self.process_folder(folder, backup_folder, workflow_id)

    async def process_folder(self, source_folder: str, backup_folder: str, workflow_id: str):
        files_to_update = await workflow.execute_activity(
            list_files_activity,
            args=[source_folder, backup_folder],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        
        if len(files_to_update) > 0:
            result, files_copied = await workflow.execute_activity(
                copy_files_activity,
                args=[files_to_update, source_folder, workflow_id],
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(seconds=1),
            )
            self.files_copied = files_copied
            if result == "PAUSE":
                
                # print(f"Pausing after copying {self.files_copied} files from {source_folder}")
                self.paused = True
                await workflow.wait_condition(lambda: not self.paused)
                # await workflow.
                # print("Workflow resumed.")
                # raise Exception

        # Copy remaining files
        if len(files_to_update) > self.files_copied:
            result = await workflow.execute_activity(
                copy_files_activity,
                args=[files_to_update[self.files_copied:], source_folder, workflow_id],
                start_to_close_timeout=timedelta(minutes=30),
                heartbeat_timeout=timedelta(seconds=30),
            )
            self.files_copied = len(files_to_update)

        print(f"Finished processing all {self.files_copied} files from {source_folder}")

    

    @workflow.signal
    def pause_backup(self):
        print("Received signal to pause backup")
        self.paused = True

    @workflow.signal
    def resume_backup(self):
        print("Received signal to resume backup")
        self.paused = False


# main method
async def main():
    client = await Client.connect("localhost:7233")
    print("A connection to the Temporal server is established")
    
    source_folders = [
        "/Users/aasthathorat/temporal-project/source1"
        # "/Users/aasthathorat/temporal-project/source4",
        #  "/Users/aasthathorat/temporal-project/source2",
        #  "/Users/aasthathorat/temporal-project/source3",
        #  "/Users/aasthathorat/temporal-project/source6"

    ]
    backup_folder = "/Users/aasthathorat/temporal-project/backup2"

   

    worker = Worker(
        client,
        task_queue="file-backup-task-queue",
        workflows=[FileBackupWorkflow],
        activities=[list_files_activity, copy_files_activity , skip_task],
    )

    async with worker:
        workflow_id = f"file-backup-{uuid.uuid4()}"
        # workflow_id = "file_backup_workflow-2"
        handle = await client.start_workflow(
            FileBackupWorkflow.run,
            args=[source_folders, backup_folder, workflow_id],
            id=workflow_id,
            task_queue="file-backup-task-queue",
            task_timeout=timedelta(seconds=30) 
        )
        
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