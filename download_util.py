from collections import deque
from datetime import date, datetime
from typing import Optional
import time

import requests
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

import db

include = ["bottomWrestler.team", "topWrestler.team", "weightClass", "event"]

PAGE_SIZE = 40


class RequestTracker:
	"""Track API request rate over time windows."""

	def __init__(self):
		# Store request timestamps (Unix timestamps)
		self.request_times = deque()

	def record_request(self):
		"""Record a new API request timestamp."""
		current_time = time.time()
		self.request_times.append(current_time)
		self.cleanup()

	def cleanup(self):
		"""Remove timestamps older than 15 minutes (900 seconds)."""
		current_time = time.time()
		cutoff_time = current_time - 900  # 15 minutes

		# Remove old entries from the left (oldest)
		while self.request_times and self.request_times[0] < cutoff_time:
			self.request_times.popleft()

	def requests_per_minute(self) -> int:
		"""Calculate number of requests in the last 60 seconds."""
		if not self.request_times:
			return 0

		current_time = time.time()
		cutoff_time = current_time - 60

		return sum(1 for req_time in self.request_times if req_time >= cutoff_time)

	def requests_per_15_minutes(self) -> int:
		"""Calculate number of requests in the last 900 seconds (15 minutes)."""
		return len(self.request_times)

def get_lookup(id: str, lookup: dict, default=None):
	if id is None:
		return default
	return lookup.get(id, default)

def _normalize_weight_filters(weight_classes):
	if not weight_classes:
		return None
	normalized = {str(weight).strip().lower() for weight in weight_classes if str(weight).strip()}
	return normalized or None


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
	if not value:
		return None
	sanitized = value.replace("Z", "+00:00")
	try:
		return datetime.fromisoformat(sanitized)
	except ValueError:
		return None


def _normalize_date_bound(value: Optional[str]) -> Optional[date]:
	if not value:
		return None
	parsed = _parse_iso_datetime(value)
	return parsed.date() if parsed else None


def _resolve_match_timestamp(data, match) -> tuple[Optional[datetime], Optional[str]]:
	attributes = match["attributes"]
	for key in ("goDateTime", "startDateTime", "endDateTime"):
		raw_value = attributes.get(key)
		parsed = _parse_iso_datetime(raw_value)
		if parsed:
			return parsed, raw_value

	event_id = attributes.get("eventId")
	for obj in data.get("included", []):
		if obj.get("type") != "event":
			continue
		if event_id and obj.get("id") != event_id:
			continue
		event_attrs = obj.get("attributes", {})
		for key in ("startDateTime", "endDateTime"):
			raw_value = event_attrs.get(key)
			parsed = _parse_iso_datetime(raw_value)
			if parsed:
				return parsed, raw_value
	return None, None


def _extract_match_date(data, match) -> Optional[date]:
	timestamp, _ = _resolve_match_timestamp(data, match)
	return timestamp.date() if timestamp else None


def _is_date_allowed(match_date: Optional[date], start_date: Optional[date], end_date: Optional[date]) -> bool:
	if match_date is None:
		return start_date is None and end_date is None
	if start_date and match_date < start_date:
		return False
	if end_date and match_date > end_date:
		return False
	return True


def update_db(
	data,
	this_id: str,
	allowed_weights=None,
	start_date: Optional[date] = None,
	end_date: Optional[date] = None,
):
	lookup = {}

	opponents = set()

	for obj in data.get("included", []):
		lookup[obj["id"]] = obj
		if obj["type"] == "team":
			db.create_team(obj["attributes"]["identityTeamId"], obj["attributes"]["name"])
		elif obj["type"] == "event":
			db.create_event(id=obj["id"], name=obj["attributes"]["name"], date=obj["attributes"].get("startDateTime",obj["attributes"].get("endDateTime", None)), location=obj["attributes"]["location"]["name"])
	
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

		if top_wrestler is None or bottom_wrestler is None:
			continue

		top_attrs = top_wrestler.get("attributes", {})
		bottom_attrs = bottom_wrestler.get("attributes", {})
		top_person_id = top_attrs.get("identityPersonId")
		bottom_person_id = bottom_attrs.get("identityPersonId")
		if top_person_id is None or bottom_person_id is None:
			continue

		weight_class_id = match["attributes"].get("weightClassId", None)
		weight_class_name = lookup.get(weight_class_id, {}).get("attributes", {}).get("name", None)
		if not weight_class_name:
			continue
		if allowed_weights and weight_class_name.lower() not in allowed_weights:
			continue
		match_datetime = _extract_match_date(data, match)
		if not _is_date_allowed(match_datetime, start_date, end_date):
			continue
		opponent_id = top_person_id if top_person_id != this_id else bottom_person_id
		opponents.add(opponent_id)

		winner_wrestler = top_wrestler if match["attributes"]["winnerWrestlerId"] == top_wrestler.get("id", None) else bottom_wrestler
		winner_attrs = winner_wrestler.get("attributes", {})
		winner_person_id = winner_attrs.get("identityPersonId")
		if winner_person_id is None:
			continue
		_, match_timestamp = _resolve_match_timestamp(data, match)

		db.create_match(
			id=match["id"],
			topWrestler_id=top_person_id,
			bottomWrestler_id=bottom_person_id,
			winner_id=winner_person_id,
			weightClass=weight_class_name,
			event_id=match["attributes"].get("eventId", None),
			date=match_timestamp,
			result=match["attributes"].get("result"),
			winType=match["attributes"].get("winType"))
	
	db.mark_fetch(this_id)
	return opponents

def download_matches(id, allowed_weights=None, start_date: Optional[str] = None, end_date: Optional[str] = None, tracker: Optional[RequestTracker] = None):
	offset = 0
	data = None
	cur = f"https://floarena-api.flowrestling.org/bouts/?identityPersonId={id}&page[size]={PAGE_SIZE}&page[offset]={offset}&hasResult=true&include=" + ",".join(include)

	cur += "&fields[wrestler]=firstName,lastName,teamId,identityPersonId"
	cur += "&fields[team]=name,identityTeamId"
	cur += "&fields[event]=name,startDateTime,location"
	cur += "&fields[weightClass]=name"
	cur += "&fields[bout]=topWrestlerId,bottomWrestlerId,weightClassId,eventId,goDateTime,startDateTime,endDateTime,result,winnerWrestlerId,winType,eventId"

	# Track the request
	if tracker:
		tracker.record_request()

	req = requests.get(cur)
	if req.status_code != 200:
		error_msg = f"Failed to download matches for id {id}: {req.status_code} {req.text}"
		if tracker:
			req_per_min = tracker.requests_per_minute()
			req_per_15min = tracker.requests_per_15_minutes()
			error_msg += f"\n\nRequest rate at failure:\n  Requests/minute: {req_per_min}\n  Requests/15min: {req_per_15min}"
		raise Exception(error_msg)
	data = req.json()

	start_bound = _normalize_date_bound(start_date)
	end_bound = _normalize_date_bound(end_date)
	opponents = update_db(data, id, allowed_weights, start_bound, end_bound)

	while "next" in data["links"] and data["links"]["next"] != cur:
		cur = data["links"]["next"]

		# Track the request
		if tracker:
			tracker.record_request()

		req = requests.get(cur)
		if req.status_code != 200:
			error_msg = f"Failed to download matches for id {id}: {req.status_code}"
			if tracker:
				req_per_min = tracker.requests_per_minute()
				req_per_15min = tracker.requests_per_15_minutes()
				error_msg += f"\n\nRequest rate at failure:\n  Requests/minute: {req_per_min}\n  Requests/15min: {req_per_15min}"
			raise Exception(error_msg)
		next_data = req.json()
		opponents.update(update_db(next_data, id, allowed_weights, start_bound, end_bound))
		data = next_data

	return opponents

def crawl(seed_id: str, depth: int = 10, reset: bool = False, allowed_weights=None, start_date: Optional[str] = None, end_date: Optional[str] = None):
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

	# Create request tracker to monitor API request rates
	tracker = RequestTracker()

	with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn(
            "• depth={task.fields[depth]} "
            "queue={task.fields[queue]} "
            "processed={task.completed} "
            "req/min={task.fields[reqs_per_min]} "
            "req/15min={task.fields[reqs_per_15min]}"
        ),
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
		task_id = progress.add_task(
            "Crawling wrestlers",
            depth=0,
            queue=len(queue),
            total=None,
            reqs_per_min=0,
            reqs_per_15min=0,
        )

		while queue:
			wrestler_id, current_depth = queue[0]

			progress.update(
                task_id,
                depth=current_depth,
                queue=len(queue),
                reqs_per_min=tracker.requests_per_minute(),
                reqs_per_15min=tracker.requests_per_15_minutes(),
            )

			if current_depth >= depth:
				queue.popleft()
				db.remove_from_queue(wrestler_id)
				continue

			if wrestler_id in processed:
				queue.popleft()
				db.remove_from_queue(wrestler_id)
				continue

			opponents = download_matches(wrestler_id, allowed_weight_filters, start_date, end_date, tracker)
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