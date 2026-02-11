from __future__ import annotations
from typing import Any, Dict, List
import logging
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
        self.status_updates_api = asana.StatusUpdatesApi(self.api_client)

    def _consume_iterator(self, iterator: Any) -> List[Dict[str, Any]]:
        """
        Safely consume an iterator from Asana SDK.
        Handles:
        1. PageIterator with .items() (returns list of dicts)
        2. Generator/Iterator yielding pages (list of dicts) -> needs flattening
        3. Generator/Iterator yielding items (dicts) -> return as is
        """
        if hasattr(iterator, "items"):
            return list(iterator.items())

        # It's a generator or generic iterator
        results = list(iterator)
        if not results:
            return []

        # Check if it yielded pages (lists) or items (dicts)
        if isinstance(results[0], list):
            # Flatten pages
            return [item for page in results for item in page]
        
        return results

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def list_projects(self, workspace_gid: str) -> List[Dict[str, Any]]:
        # Only GET operations
        opts = {"archived": False, "limit": 100}
        projects = self.projects_api.get_projects_for_workspace(workspace_gid, opts=opts)
        return self._consume_iterator(projects)

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
        return self._consume_iterator(tasks)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def list_status_updates(self, project_gid: str) -> List[Dict[str, Any]]:
        opts = {"limit": 100, "opt_fields": ",".join([
            "gid",
            "created_at",
            "modified_at",
            "text",
            "html_text",
            "title",
            "status_type",
            "author.gid",
            "author.name",
        ])}
        updates = self.status_updates_api.get_statuses_for_object(project_gid, opts=opts)
        return self._consume_iterator(updates)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def list_status_update_comments(self, status_update_gid: str) -> List[Dict[str, Any]]:
        # Use generic /stories endpoint with parent parameter
        try:
            query_params = {
                "parent": status_update_gid,
                "opt_fields": ",".join([
                    "gid",
                    "created_at",
                    "text",
                    "html_text",
                    "created_by.gid",
                    "created_by.name",
                    "type",
                    "resource_subtype",
                ])
            }
            resp = self.api_client.call_api(
                "/stories",
                "GET",
                query_params=query_params,
                response_type=object,
                auth_settings=["personalAccessToken"],
                _return_http_data_only=True,
            )
            if isinstance(resp, dict):
                data = resp.get("data")
            elif isinstance(resp, list):
                data = resp
            else:
                data = getattr(resp, "data", None)
            
            # Filter for comments if needed, or return all stories. 
            # Usually users only want 'comment' stories.
            all_stories = list(data or [])
            return [s for s in all_stories if s.get("type") == "comment"]
        except Exception as e:
            logging.getLogger("asana").warning(
                "Failed to parse status update comments response. status_update=%s err=%s",
                status_update_gid, e
            )
            return []

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def search_tasks(
        self,
        workspace_gid: str,
        text_query: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Search tasks in a workspace using /tasks/search.
        Note: returns tasks, not projects. Use memberships.project to map back.
        """
        results: List[Dict[str, Any]] = []
        params = {
            "text": text_query,
            "limit": limit,
            "opt_fields": ",".join([
                "gid",
                "name",
                "memberships.project.gid",
                "memberships.project.name",
            ]),
        }
        next_page = None
        while True:
            if next_page and next_page.get("offset"):
                params["offset"] = next_page["offset"]
            resp = self.api_client.call_api(
                f"/workspaces/{workspace_gid}/tasks/search",
                "GET",
                query_params=params,
                response_type=object,
                auth_settings=["personalAccessToken"],
                _return_http_data_only=True,
            )
            data = None
            if isinstance(resp, dict):
                data = resp.get("data")
                next_page = resp.get("next_page")
            elif isinstance(resp, list):
                data = resp
                next_page = None
            else:
                data = getattr(resp, "data", None)
                next_page = getattr(resp, "next_page", None)

            if data:
                results.extend(list(data))
            if not next_page:
                break
        return results
