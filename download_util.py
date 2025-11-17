import requests
import sqlite3
from collections import deque

from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from collections import deque

import db

include = ["bottomWrestler.team", "topWrestler.team", "weightClass", "event"]

PAGE_SIZE = 40

def get_lookup(id: str, lookup: dict, default=None):
	if id is None:
		return default
	return lookup.get(id, default)

def update_db(data, this_id: str):
	lookup = {}

	opponents = set()

	for obj in data["included"]:
		lookup[obj["id"]] = obj
		if obj["type"] == "team":
			db.create_team(obj["attributes"]["identityTeamId"], obj["attributes"]["name"])
		elif obj["type"] == "event":
			db.create_event(id=obj["id"], name=obj["attributes"]["name"], date=obj["attributes"]["startDateTime"], location=obj["attributes"]["location"]["name"])
	
	for obj in data["included"]:
		if obj["type"] == "wrestler":
			db.create_wrestler(
				id=obj["attributes"]["identityPersonId"],
				name=obj["attributes"]["firstName"] + " " + obj["attributes"]["lastName"],
				team_id=get_lookup(obj["attributes"]["teamId"], lookup, {}).get("attributes", {}).get("identityTeamId", None)
			)

	for match in data["data"]:
		if match["attributes"]["winType"] == "BYE":
			continue
		top_wrestler = get_lookup(match["attributes"].get("topWrestlerId", None), lookup, None)
		bottom_wrestler = get_lookup(match["attributes"].get("bottomWrestlerId", None), lookup, None)

		if (top_wrestler is not None) and (bottom_wrestler is not None):
			opponents.add(top_wrestler["attributes"]["identityPersonId"] if match["attributes"]["topWrestlerId"] != this_id else bottom_wrestler["attributes"]["identityPersonId"])

			winner_wrestler = top_wrestler if match["attributes"]["winnerWrestlerId"] == top_wrestler.get("id", None) else bottom_wrestler

			db.create_match(
				id=match["id"],
				topWrestler_id=top_wrestler["attributes"]["identityPersonId"] if top_wrestler else None,
				bottomWrestler_id=bottom_wrestler["attributes"]["identityPersonId"] if bottom_wrestler else None,
				winner_id=winner_wrestler["attributes"]["identityPersonId"],
				weightClass=lookup.get(match["attributes"]["weightClassId"], []).get("attributes", []).get("name", None),
				event_id=match["attributes"].get("eventId", None),
				matchDate=match["attributes"].get("goDateTime", None),
				result=match["attributes"]["result"],
				winType=match["attributes"]["winType"])
	
	return opponents

def download_matches(id):
	offset = 0
	data = None
	cur = f"https://floarena-api.flowrestling.org/bouts/?identityPersonId={id}&page[size]={PAGE_SIZE}&page[offset]={offset}&hasResult=true&include=" + ",".join(include)

	cur += "&fields[wrestler]=firstName,lastName,teamId,identityPersonId"
	cur += "&fields[team]=name,identityTeamId"
	cur += "&fields[event]=name,startDateTime,location"
	cur += "&fields[weightClass]=name"
	cur += "&fields[bout]=topWrestlerId,bottomWrestlerId,weightClassId,eventId,goDateTime,result,winnerWrestlerId,winType"

	req = requests.get(cur)
	if req.status_code != 200:
		raise Exception(f"Failed to download matches for id {id}: {req.status_code} {req.text}")
	data = req.json()

	opponents = update_db(data, id)

	while "next" in data["links"] and data["links"]["next"] != cur:
		cur = data["links"]["next"]
		req = requests.get(cur)
		if req.status_code != 200:
			raise Exception(f"Failed to download matches for id {id}: {req.status_code}")
		next_data = req.json()
		opponents.update(update_db(next_data, id))
	
	return opponents

def crawl(seed_id: str, depth: int = 10):
	queue = deque()
	fetched = set()
	queue.append((seed_id, 0))
	seen = {seed_id}
	depths = {seed_id: 0}

	with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn(
            "• depth={task.fields[depth]} "
            "queue={task.fields[queue]} "
            "processed={task.completed}"
        ),
        TimeElapsedColumn(),
        transient=True,  # clear bar when done
    ) as progress:
		task_id = progress.add_task(
            "Crawling wrestlers",
            depth=0,
            queue=len(queue),
            total=None,  # unknown total
        )
		
		while queue:
			wrestler_id, current_depth = queue.popleft()

			if current_depth >= depth:
				continue

			if wrestler_id in fetched:
				continue

            # update fields shown in the bar
			progress.update(
                task_id,
                depth=current_depth,
                queue=len(queue),
            )

            # do the actual work
			opponents = download_matches(wrestler_id)
			fetched.add(wrestler_id)
			progress.advance(task_id, 1)

			for opponent_id in opponents:
				if opponent_id not in seen:
					seen.add(opponent_id)
					depths[opponent_id] = current_depth + 1
					queue.append((opponent_id, current_depth + 1))

                    # keep queue size up to date in the display
					progress.update(
						task_id,
						queue=len(queue),
					)

	return {
		"seen": seen,
		"depths": depths,
		"fetched": fetched,
	}