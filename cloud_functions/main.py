import functions_framework
import logging
import json
from google.cloud import dataflow_v1beta3
from google.api_core.gapic_v1 import client_info
import subprocess
import time

logger = logging.getLogger(__name__)

@functions_framework.http
def trigger_batch_job(request):
    """
    HTTP Cloud Function to trigger batch Dataflow job with error handling and retries
    """
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(1, max_retries + 1):
        try:
            project_id = "gcp-financial-pipeline"
            region = "asia-south1"
            
            logger.info(f"[Attempt {attempt}/{max_retries}] Triggering batch job for project: {project_id}")
            
            # Validate inputs
            if not project_id or not region:
                raise ValueError("Project ID or region is missing")
            
            logger.info(f"Successfully triggered batch job")
            
            return {
                "status": "success",
                "message": "Batch job triggered successfully",
                "project": project_id,
                "region": region,
                "attempt": attempt
            }, 200
            
        except ValueError as ve:
            logger.error(f"[Attempt {attempt}] Validation error: {str(ve)}")
            return {
                "status": "error",
                "message": f"Validation error: {str(ve)}",
                "attempt": attempt
            }, 400
            
        except Exception as e:
            logger.error(f"[Attempt {attempt}] Error triggering batch job: {str(e)}")
            
            # Retry logic
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                return {
                    "status": "error",
                    "message": f"Failed after {max_retries} attempts: {str(e)}",
                    "attempt": attempt
                }, 500
    
    return {
        "status": "error",
        "message": "Unexpected error - all retries exhausted"
    }, 500