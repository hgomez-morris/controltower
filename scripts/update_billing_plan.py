
import os
import sys
import logging
import argparse
import time
from typing import List, Dict, Optional

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

import asana
from controltower.config import load_config

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("update_billing_plan")

class AsanaUpdater:
    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.access_token = os.getenv("ASANA_ACCESS_TOKEN")
        if not self.access_token:
            raise ValueError("ASANA_ACCESS_TOKEN env var is not set")
        
        configuration = asana.Configuration()
        configuration.access_token = self.access_token
        self.api_client = asana.ApiClient(configuration)
        self.projects_api = asana.ProjectsApi(self.api_client)
        self.custom_fields_api = asana.CustomFieldsApi(self.api_client)
        self.workspace_gid = self.config["asana"]["workspace_gid"]
        self.pmo_id_map: Dict[str, dict] = {}

    def build_pmo_id_map(self):
        """Fetches all projects and builds a map of PMO ID -> Project."""
        logger.info("Fetching all projects to build PMO ID map...")
        opts = {
            "workspace": self.workspace_gid,
            "archived": False,
            "limit": 100,
            "opt_fields": "name,gid,current_status.color,current_status.text,custom_fields.gid,custom_fields.name,custom_fields.text_value,custom_fields.display_value,custom_fields.enum_options"
        }
        
        iterator = self.projects_api.get_projects(opts=opts)
        
        projects = []
        if hasattr(iterator, "items"):
            projects = list(iterator.items())
        else:
            raw = list(iterator)
            if raw and isinstance(raw[0], list):
                 projects = [item for page in raw for item in page]
            else:
                 projects = raw
        
        logger.info(f"Fetched {len(projects)} projects. Indexing...")
        
        PMO_ID_GID = "1208154537807104"
        
        for p in projects:
            pmo_id_value = None
            for cf in p.get("custom_fields", []):
                if cf["gid"] == PMO_ID_GID:
                    pmo_id_value = cf.get("text_value") or cf.get("display_value")
                    break
            
            if pmo_id_value:
                clean_id = pmo_id_value.strip()
                if clean_id in self.pmo_id_map:
                    logger.warning(f"Duplicate PMO ID found: {clean_id} (Projects: {self.pmo_id_map[clean_id]['gid']}, {p['gid']})")
                self.pmo_id_map[clean_id] = p
        
        logger.info(f"Indexed {len(self.pmo_id_map)} projects by PMO ID.")

    def find_project_by_pmo_id(self, pmo_id_target: str) -> Optional[dict]:
        """Finds a project by PMO ID using the pre-built map."""
        if not self.pmo_id_map:
            self.build_pmo_id_map()
            
        p = self.pmo_id_map.get(pmo_id_target)
        if p:
            logger.info(f"MATCH FOUND: {p['name']} ({p['gid']})")
            self._print_project_details(p)
            return p
        
        logger.warning(f"Project with PMO ID '{pmo_id_target}' not found.")
        return None

    def _print_project_details(self, project: dict):
        status = project.get("current_status") or {}
        status_text = f"{status.get('color')} ({status.get('text')})" if status else "No Status"
        
        billing_plan = "N/A"
        for cf in project.get("custom_fields", []):
            if cf["name"] == "En plan de facturación":
                billing_plan = cf.get("display_value") or "Empty"
                break
        
        print("\n" + "="*60)
        print(f"Project: {project['name']}")
        print(f"GID:     {project['gid']}")
        print(f"Status:  {status_text}")
        print(f"En plan de facturación: {billing_plan}")
        print("="*60 + "\n")

    def get_custom_field_gid_by_name(self, project_gid: str, field_name: str) -> Optional[str]:
        """Gets the GID of a custom field in a project by its name."""
        try:
            # We already have CFs from the find call usually, but if we need to refresh or if passed only GID
            opts = {"opt_fields": "custom_fields.name,custom_fields.gid,custom_fields.enum_options"}
            project = self.projects_api.get_project(project_gid, opts=opts)
            
            for cf in project.get("custom_fields", []):
                if cf["name"] == field_name:
                    return cf["gid"]
            
            logger.warning(f"Custom field '{field_name}' not found in project {project_gid}")
            return None
        except Exception as e:
            logger.error(f"Error getting custom fields for project {project_gid}: {e}")
            return None

    def get_enum_option_gid(self, custom_field_gid: str, option_name: str) -> Optional[str]:
        """Gets the GID of an enum option for a custom field."""
        try:
            # We can get the custom field definition directly
            cf = self.custom_fields_api.get_custom_field(custom_field_gid, opts={})
            for opt in cf.get("enum_options", []):
                if opt["name"] == option_name:
                    return opt["gid"]
            
            opts_available = [opt["name"] for opt in cf.get("enum_options", [])]
            logger.warning(f"Option '{option_name}' not found in custom field {custom_field_gid}. Available: {opts_available}")
            return None
        except Exception as e:
            logger.error(f"Error getting custom field options: {e}")
            return None

    def update_project_custom_field(self, project_gid: str, field_name: str, value: str):
        """Updates a custom field in a project."""
        logger.info(f"Updating project {project_gid} - Field '{field_name}' to '{value}'")
        
        # 1. Get Custom Field GID
        cf_gid = self.get_custom_field_gid_by_name(project_gid, field_name)
        if not cf_gid:
            logger.error("Could not find custom field GID. Aborting update.")
            return

        # 2. Check type (Text or Enum) and prepare value
        # For simplicity, let's check if it's an enum by trying to find the option.
        # Ideally we check the type from the CF definition.
        cf_def = self.custom_fields_api.get_custom_field(cf_gid, opts={})
        cf_type = cf_def.get("type")
        
        payload = {"data": {"custom_fields": {cf_gid: None}}}
        
        if cf_type == "enum":
            enum_gid = self.get_enum_option_gid(cf_gid, value)
            if not enum_gid:
                logger.error(f"Could not find enum option '{value}' for field '{field_name}'.")
                return
            payload["data"]["custom_fields"][cf_gid] = enum_gid
        elif cf_type in ("text", "number"):
            payload["data"]["custom_fields"][cf_gid] = value
        else:
            logger.error(f"Unsupported custom field type: {cf_type}")
            return

        # 3. Update Project
        try:
            self.projects_api.update_project(payload, project_gid, opts={})
            logger.info(f"Successfully updated project {project_gid}")
        except Exception as e:
            logger.error(f"Failed to update project {project_gid}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Update 'En plan de facturación' field in Asana projects.")
    parser.add_argument("--project", help="PMO ID of the project (e.g. PMO-1115)")
    parser.add_argument("--list-file", help="Path to file with list of PMO IDs")
    parser.add_argument("--value", default="Sí", help="Value to set for 'En plan de facturación'")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually update, just check")
    parser.add_argument("--no-read-only", action="store_true", help="Perform the update (disables read-only mode)")

    args = parser.parse_args()
    read_only = not args.no_read_only

    updater = AsanaUpdater("config/config.local.yaml" if os.path.exists("config/config.local.yaml") else "config/config.example.yaml")
    
    target_projects = []
    
    if args.project:
        target_projects.append(args.project)
    
    if args.list_file:
        try:
            with open(args.list_file, "r", encoding="utf-8") as f:
                lines = [l.strip() for l in f.readlines() if l.strip()]
                target_projects.extend(lines)
        except Exception as e:
            logger.error(f"Could not read list file: {e}")
            return

    if not target_projects:
        logger.error("No projects specified. Use --project or --list-file")
        return

    logger.info(f"Targeting {len(target_projects)} PMO IDs.")
    
    FIELD_NAME = "En plan de facturación"
    
    for pmo_id in target_projects:
        logger.info(f"Processing PMO ID: {pmo_id}")
        project = updater.find_project_by_pmo_id(pmo_id)
        
        if not project:
            continue
            
        if read_only:
            logger.info("READ-ONLY mode: Skipping update.")
            continue

        if args.dry_run:
            logger.info(f"[DRY RUN] Would update {project['name']} ({project['gid']})")
        else:
            updater.update_project_custom_field(project['gid'], FIELD_NAME, args.value)
            time.sleep(1) # Rate limit niceness

if __name__ == "__main__":
    main()
