from __future__ import annotations
import os
from typing import Any, Dict, List, Optional
import asana
from tenacity import retry, stop_after_attempt, wait_exponential

READ_ONLY_NOTE = "MVP is READ-ONLY: do not use create/update/delete endpoints."

class AsanaReadOnlyClient:
    def __init__(self, access_token: str) -> None:
        self.client = asana.Client.access_token(access_token)
        # optional: client options here

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def list_projects(self, workspace_gid: str) -> List[Dict[str, Any]]:
        # Only GET operations
        projects = self.client.projects.get_projects({"workspace": workspace_gid, "archived": False}, opt_pretty=False)
        return list(projects)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def get_project(self, project_gid: str) -> Dict[str, Any]:
        return self.client.projects.get_project(project_gid, opt_pretty=False)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def get_project_tasks(self, project_gid: str) -> List[Dict[str, Any]]:
        # Use fields minimal to reduce payload; adjust as needed
        tasks = self.client.tasks.get_tasks_for_project(project_gid, {"opt_fields": "completed,created_at,completed_at,modified_at"}, opt_pretty=False)
        return list(tasks)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def get_project_statuses(self, project_gid: str) -> List[Dict[str, Any]]:
        statuses = self.client.project_statuses.get_statuses_for_project(project_gid, opt_pretty=False)
        return list(statuses)
