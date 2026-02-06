from __future__ import annotations
import json
import os
import sys
from datetime import datetime

import boto3

"""
Probe AWS Partner Central Benefits to discover funding/amount fields.

Usage (PowerShell):
  # with env creds already set (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
  python scripts/aws_benefits_probe.py O7674290

Optional env vars:
  AWS_REGION=us-east-1
  AWS_ROLE_ARN=arn:aws:iam::058106090841:role/morrisopazo   (assume role)
  OPPORTUNITY_ARN=arn:...                                   (if you already know it)
  OUTPUT=benefits_probe.json
"""


def assume_role_if_needed(region: str) -> None:
    role_arn = os.getenv("AWS_ROLE_ARN")
    if not role_arn:
        return
    sts = boto3.client("sts", region_name=region)
    resp = sts.assume_role(RoleArn=role_arn, RoleSessionName="pmo-benefits-probe")
    creds = resp["Credentials"]
    os.environ["AWS_ACCESS_KEY_ID"] = creds["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds["SecretAccessKey"]
    os.environ["AWS_SESSION_TOKEN"] = creds["SessionToken"]


def _safe_get(op: dict, *path, default=None):
    cur = op
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/aws_benefits_probe.py <OPPORTUNITY_ID>")
        return 1

    opportunity_id = sys.argv[1].strip()
    region = os.getenv("AWS_REGION", "us-east-1")
    catalog = os.getenv("AWS_CATALOG", "AWS")
    output = os.getenv("OUTPUT", "benefits_probe.json")

    assume_role_if_needed(region)

    selling = boto3.client("partnercentral-selling", region_name=region)
    benefits = boto3.client("partnercentral-benefits", region_name=region)

    # Try to read opportunity to get ARN (if present)
    opportunity = None
    opportunity_arn = os.getenv("OPPORTUNITY_ARN", "").strip() or None
    try:
        opportunity = selling.get_opportunity(Catalog=catalog, Identifier=opportunity_id)
        opportunity_arn = opportunity_arn or _safe_get(opportunity, "Opportunity", "Arn") or _safe_get(opportunity, "Opportunity", "Id")
    except Exception as e:
        opportunity = {"error": str(e)}

    # List benefit applications (paginate)
    applications = []
    token = None
    while True:
        kwargs = {"MaxResults": 50, "Catalog": catalog}
        if token:
            kwargs["NextToken"] = token
        resp = benefits.list_benefit_applications(**kwargs)
        applications.extend(resp.get("BenefitApplications", []))
        token = resp.get("NextToken")
        if not token:
            break

    # Filter applications associated with the opportunity
    matched_apps = []
    for app in applications:
        assoc = app.get("AssociatedResources") or []
        if not assoc:
            continue
        if opportunity_arn and any(opportunity_arn in (r.get("Arn") or "") for r in assoc):
            matched_apps.append(app)
            continue
        # fallback: match by opportunity id in ARN
        if any(opportunity_id in (r.get("Arn") or "") for r in assoc):
            matched_apps.append(app)
            continue

    # For each app, fetch details and allocations
    results = []
    for app in matched_apps:
        app_id = app.get("Id") or app.get("Identifier")
        if not app_id:
            continue
        app_details = benefits.get_benefit_application(Catalog=catalog, Identifier=app_id)

        allocations = []
        token = None
        while True:
            kwargs = {"BenefitApplicationIdentifier": app_id, "MaxResults": 50, "Catalog": catalog}
            if token:
                kwargs["NextToken"] = token
            alloc_resp = benefits.list_benefit_allocations(**kwargs)
            for alloc in alloc_resp.get("BenefitAllocations", []):
                alloc_id = alloc.get("Id") or alloc.get("Identifier")
                if not alloc_id:
                    continue
                alloc_details = benefits.get_benefit_allocation(Catalog=catalog, Identifier=alloc_id)
                allocations.append(alloc_details)
            token = alloc_resp.get("NextToken")
            if not token:
                break

        results.append({
            "application": app,
            "application_details": app_details,
            "allocations": allocations,
        })

    out = {
        "opportunity_id": opportunity_id,
        "opportunity_arn": opportunity_arn,
        "opportunity": opportunity,
        "applications_total": len(applications),
        "applications_matched": len(matched_apps),
        "results": results,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

    with open(output, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, default=str)

    print(f"Saved to {output}. Matched apps: {len(matched_apps)} (total apps: {len(applications)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
