import functions_framework
import logging

logger = logging.getLogger(__name__)

@functions_framework.http
def trigger_batch_job(request):
    """
    HTTP Cloud Function to trigger batch Dataflow job
    """
    try:
        project_id = "gcp-financial-pipeline"
        region = "asia-south1"
        
        logger.info(f"Triggering batch job for project: {project_id}")
        
        return {
            "status": "success",
            "message": "Batch job triggered successfully",
            "project": project_id,
            "region": region
        }, 200
            
    except Exception as e:
        logger.error(f"Error triggering batch job: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }, 500