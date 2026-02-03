from __future__ import annotations
from typing import Any, Dict, List
import asana
from tenacity import retry, stop_after_attempt, wait_exponential

READ_ONLY_NOTE = "MVP is READ-ONLY: do not use create/update/delete endpoints."

class AsanaReadOnlyClient:
    def __init__(self, access_token: str) -> None:
        configuration = asana.Configuration()
        configuration.access_token = access_token
        self.api_client = asana.ApiClient(configuration)
        self.projects_api = asana.ProjectsApi(self.api_client)
        self.tasks_api = asana.TasksApi(self.api_client)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def list_projects(self, workspace_gid: str) -> List[Dict[str, Any]]:
        # Only GET operations
        opts = {"archived": False, "limit": 100}
        projects = self.projects_api.get_projects_for_workspace(workspace_gid, opts=opts)
        return list(projects)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def get_project(self, project_gid: str) -> Dict[str, Any]:
        opts = {"opt_fields": ",".join([
            "name",
            "owner",
            "owner.name",
            "owner.gid",
            "due_date",
            "due_on",
            "start_on",
            "created_at",
            "modified_at",
            "completed",
            "completed_at",
            "current_status",
            "custom_fields",
            "custom_field_settings",
        ])}
        return self.projects_api.get_project(project_gid, opts=opts)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def get_project_tasks(self, project_gid: str) -> List[Dict[str, Any]]:
        # Minimal fields for metrics (counts and activity)
        opts = {"opt_fields": "completed,created_at,completed_at,modified_at", "limit": 100}
        tasks = self.tasks_api.get_tasks_for_project(project_gid, opts=opts)
        return list(tasks)
