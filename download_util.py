import requests
from collections import deque

from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

import db

include = ["bottomWrestler.team", "topWrestler.team", "weightClass", "event"]

PAGE_SIZE = 40

def get_lookup(id: str, lookup: dict, default=None):
	if id is None:
		return default
	return lookup.get(id, default)

def _normalize_weight_filters(weight_classes):
	if not weight_classes:
		return None
	normalized = {str(weight).strip().lower() for weight in weight_classes if str(weight).strip()}
	return normalized or None


def _is_weight_allowed(weight_class_id, weight_class_name, allowed_weights):
	if not allowed_weights:
		return True
	candidates = []
	if weight_class_id:
		candidates.append(str(weight_class_id).lower())
	if weight_class_name:
		candidates.append(str(weight_class_name).lower())
	return any(candidate in allowed_weights for candidate in candidates)


def update_db(data, this_id: str, allowed_weights=None):
	lookup = {}

	opponents = set()

	for obj in data.get("included", []):
		lookup[obj["id"]] = obj
		if obj["type"] == "team":
			db.create_team(obj["attributes"]["identityTeamId"], obj["attributes"]["name"])
		elif obj["type"] == "event":
			db.create_event(id=obj["id"], name=obj["attributes"]["name"], date=obj["attributes"]["startDateTime"], location=obj["attributes"]["location"]["name"])
	
	for obj in data.get("included", []):
		if obj["type"] == "wrestler":
			db.create_wrestler(
				id=obj["attributes"]["identityPersonId"],
				name=obj["attributes"]["firstName"] + " " + obj["attributes"]["lastName"],
				team_id=get_lookup(obj["attributes"]["teamId"], lookup, {}).get("attributes", {}).get("identityTeamId", None)
			)

	for match in data.get("data", []):
		if match["attributes"]["winType"] == "BYE":
			continue
		top_wrestler = get_lookup(match["attributes"].get("topWrestlerId", None), lookup, None)
		bottom_wrestler = get_lookup(match["attributes"].get("bottomWrestlerId", None), lookup, None)

		if (top_wrestler is not None) and (bottom_wrestler is not None):
			weight_class_id = match["attributes"].get("weightClassId", None)
			weight_class_name = lookup.get(weight_class_id, {}).get("attributes", {}).get("name", None)
			if not _is_weight_allowed(weight_class_id, weight_class_name, allowed_weights):
				continue
			opponents.add(top_wrestler["attributes"]["identityPersonId"] if top_wrestler["attributes"]["identityPersonId"] != this_id else bottom_wrestler["attributes"]["identityPersonId"])

			winner_wrestler = top_wrestler if match["attributes"]["winnerWrestlerId"] == top_wrestler.get("id", None) else bottom_wrestler

			db.create_match(
				id=match["id"],
				topWrestler_id=top_wrestler["attributes"]["identityPersonId"],
				bottomWrestler_id=bottom_wrestler["attributes"]["identityPersonId"],
				winner_id=winner_wrestler["attributes"]["identityPersonId"],
				weightClass=weight_class_name,
				event_id=match["attributes"].get("eventId", None),
				date=match["attributes"].get("goDateTime", None),
				result=match["attributes"]["result"],
				winType=match["attributes"]["winType"])
	
	db.mark_fetch(this_id)
	return opponents

def download_matches(id, allowed_weights=None):
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

	opponents = update_db(data, id, allowed_weights)

	while "next" in data["links"] and data["links"]["next"] != cur:
		cur = data["links"]["next"]
		req = requests.get(cur)
		if req.status_code != 200:
			raise Exception(f"Failed to download matches for id {id}: {req.status_code}")
		next_data = req.json()
		opponents.update(update_db(next_data, id, allowed_weights))
		data = next_data
	
	return opponents

def crawl(seed_id: str, depth: int = 10, reset: bool = False, allowed_weights=None):
	if reset:
		db.clear_crawler_state()

	allowed_weight_filters = _normalize_weight_filters(allowed_weights)

	state = db.get_crawler_state()
	depth_upgraded = False
	previous_depth = state["depth_limit"] if state else 0
	seed_changed = state is not None and state["seed_id"] != seed_id
	depth_upgraded = depth > previous_depth
	db.upsert_crawler_state(seed_id, depth)

	if seed_changed:
		db.clear_queue()

	queue_items = db.get_queue_items()
	queue = deque(queue_items)
	queued_ids = {item[0] for item in queue_items}
	seen_rows = db.get_seen_wrestlers()
	seen = {row[0] for row in seen_rows}
	depths = {row[0]: row[1] for row in seen_rows}
	processed = set(db.get_processed_wrestlers())

	# Always refetch the seed wrestler even if previously crawled
	seen.discard(seed_id)
	depths.pop(seed_id, None)
	processed.discard(seed_id)
	queued_ids.discard(seed_id)

	def ensure_seed_front():
		nonlocal queue
		# Remove existing seed entries, enqueue at depth 0, and place at front
		queue = deque([(wid, dep) for (wid, dep) in queue if wid != seed_id])
		db.enqueue_wrestler(seed_id, 0)
		queue.appendleft((seed_id, 0))
		queued_ids.add(seed_id)

	if depth_upgraded:
		newly_enqueued = []
		for wrestler_id, wrestler_depth in db.get_unprocessed_wrestlers(max_depth=depth):
			if wrestler_depth < previous_depth:
				continue
			if wrestler_id in processed or wrestler_id in queued_ids:
				continue
			db.enqueue_wrestler(wrestler_id, wrestler_depth)
			newly_enqueued.append((wrestler_id, wrestler_depth))
			queued_ids.add(wrestler_id)

		for wrestler_id, wrestler_depth in reversed(newly_enqueued):
			queue.appendleft((wrestler_id, wrestler_depth))

	if seed_changed:
		queue.clear()
		queued_ids.clear()

	ensure_seed_front()

	if not queue:
		replenished = []
		for wrestler_id, wrestler_depth in db.get_unprocessed_wrestlers(max_depth=depth):
			if wrestler_id in processed or wrestler_id in queued_ids:
				continue
			db.enqueue_wrestler(wrestler_id, wrestler_depth)
			replenished.append((wrestler_id, wrestler_depth))
			queued_ids.add(wrestler_id)

		if replenished:
			queue.extend(replenished)
		else:
			if seed_id not in seen:
				db.record_seen_wrestler(seed_id, 0)
				seen.add(seed_id)
				depths[seed_id] = 0
			db.enqueue_wrestler(seed_id, 0)
			queue.append((seed_id, 0))

	with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn(
            "• depth={task.fields[depth]} "
            "queue={task.fields[queue]} "
            "processed={task.completed}"
        ),
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
		task_id = progress.add_task(
            "Crawling wrestlers",
            depth=0,
            queue=len(queue),
            total=None,
        )

		while queue:
			wrestler_id, current_depth = queue[0]

			progress.update(
                task_id,
                depth=current_depth,
                queue=len(queue),
            )

			if current_depth >= depth:
				queue.popleft()
				db.remove_from_queue(wrestler_id)
				continue

			if wrestler_id in processed:
				queue.popleft()
				db.remove_from_queue(wrestler_id)
				continue

			opponents = download_matches(wrestler_id, allowed_weight_filters)
			processed.add(wrestler_id)
			db.mark_wrestler_processed(wrestler_id)
			progress.advance(task_id, 1)

			for opponent_id in opponents:
				if opponent_id not in seen:
					seen.add(opponent_id)
					next_depth = current_depth + 1
					depths[opponent_id] = next_depth
					db.record_seen_wrestler(opponent_id, next_depth)
					if next_depth <= depth:
						db.enqueue_wrestler(opponent_id, next_depth)
						queue.append((opponent_id, next_depth))
						progress.update(task_id, queue=len(queue))

			queue.popleft()
			db.remove_from_queue(wrestler_id)

	return {
		"seen": seen,
		"depths": depths,
		"fetched": processed,
	}