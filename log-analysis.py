import argparse
import itertools
import json
import os.path
import pandas as pd
import plotly.express as px
import re

from datetime import datetime

examples = 10

def parse_datetime(datetime_str):
	return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f").timestamp()

def datetime_roundup(epoch_time):
	return datetime.fromtimestamp(epoch_time).strftime("%Y-%m-%d %H:%M")

def get_transaction_events(rule):
	def get_contract(event):
		match = re.search(r"Daml\.Trigger\.LowLevel:AnyContractId@[0-9a-fA-F]+{\s*templateId\s*=\s*DA\.Internal\.Any:TemplateTypeRep@[0-9a-fA-F]+{\s*getTemplateTypeRep\s*=\s*(.+?)\s+},\s*contractId\s*=\s*([0-9a-fA-F]+)\s*}", event)
		return {"templateId": match.group(1), "contractId": match.group(2)}
	for child in rule["children"]:
		if child["name"] != 'trigger.rule.update.evaluation':
			continue
		if "children" not in child:
			continue
		for subchild in child["children"]:
			for event in subchild["events"]:
				if event["message"] != 'Trigger rule evaluation':
					continue
				if "details" not in event:
					continue
				if "message" not in event["details"]:
					continue
				archives = re.findall(r"Daml\.Trigger\.LowLevel:Event@[0-9a-fA-F]+:ArchivedEvent\(Daml\.Trigger\.LowLevel:Archived@[0-9a-fA-F]+{.*?}\)", event["details"]["message"])
				creates = re.findall(r"Daml\.Trigger\.LowLevel:Event@[0-9a-fA-F]+:CreatedEvent\(Daml\.Trigger\.LowLevel:Created@[0-9a-fA-F]+{.*?}\)", event["details"]["message"])
				return {"archives": [ get_contract(event) for event in archives ], "creates": [ get_contract(event) for event in creates ], "timestamp": parse_datetime(event["timestamp"])}
	return None

def get_transaction_arrival_time(rule):
	for event in rule["events"]:
		if event["message"] != 'Transaction source':
			continue
		return parse_datetime(event["timestamp"])
	return None

def get_completion_arrival_time(rule):
	for event in rule["events"]:
		if event["message"] != 'Completion source':
			continue
		return parse_datetime(event["timestamp"])
	return None

def get_rule_start_time(rule):
	for child in rule["children"]:
		if "children" not in child:
			continue
		for subchild in child["children"]:
			for event in subchild["events"]:
				if event["message"] != 'Batching message for rule evaluation':
					continue
				return parse_datetime(event["timestamp"])
	return None

def get_rule_end_time(rule):
	for child in rule["children"]:
		if "children" not in child:
			continue
		for subchild in child["children"]:
			for event in subchild["events"]:
				if event["message"] != 'Trigger rule state updated':
					continue
				return parse_datetime(event["timestamp"])
	return None

def get_transaction_command_id(rule):
	for child in rule["children"]:
		for event in child["events"]:
			if event["message"] != 'In-flight command completed':
				continue
			if "details" not in event:
				continue
			if "commandId" not in event["details"]:
				continue
			return event["details"]["commandId"]
	return None

def get_transaction_transaction_id(rule):
	for child in rule["children"]:
		if "children" not in child:
			continue
		for subchild in child["children"]:
			for event in subchild["events"]:
				if event["message"] != 'Trigger rule evaluation':
					continue
				if "details" not in event:
					continue
				if "message" not in event["details"]:
					continue
				return re.search(r"Daml\.Trigger\.LowLevel:TransactionId@[0-9a-fA-F]+{\s*unpack\s*=\s*([0-9a-fA-F]+)\s*}", event["details"]["message"]).group(1)
	return None

def get_completion_command_id(rule):
	for event in rule["events"]:
		if "details" not in event:
			continue
		if "message" not in event["details"]:
			continue
		if "commandId" not in event["details"]["message"]:
			continue
		return event["details"]["message"]["commandId"]
	return None

def get_completion_failure_message(rule):
	for event in rule["events"]:
		if event["message"] != 'Completion source':
			continue
		return re.sub(r"\([0-9]+,[0-9a-fA-F]+\)", "", event["details"]["message"]["message"]).strip()
	return None

def get_completion_transaction_id(rule):
	for child in rule["children"]:
		if child["name"] != 'trigger.rule.update.evaluation':
			continue
		if "children" not in child:
			continue
		for subchild in child["children"]:
			for event in subchild["events"]:
				if event["message"] != 'Trigger rule evaluation':
					continue
				if "details" not in event:
					continue
				if "message" not in event["details"]:
					continue
				return re.search(r"Daml\.Trigger\.LowLevel:TransactionId@[0-9a-fA-F]+{\s*unpack\s*=\s*([0-9a-fA-F]+)\s*}", event["details"]["message"]).group(1)
	return None

def get_submission_command(rule):
	for child in rule["children"]:
		if "children" not in child:
			continue
		for subchild in child["children"]:
			for event in subchild["events"]:
				if event["message"] != 'New in-flight command':
					continue
				if "details" not in event:
					continue
				if "commandId" not in event["details"]:
					continue
				if "commands" not in event["details"]:
					continue
				return (event["details"]["commandId"], {"commands": event["details"]["commands"], "timestamp": parse_datetime(event["timestamp"])})
	return None

def get_completion_timestamp(rule):
	for event in rule["events"]:
		if event["message"] == 'Completion source':
			return parse_datetime(event["timestamp"])
	return None

def get_transaction_timestamp(rule):
	for event in rule["events"]:
		if event["message"] == 'Transaction source':
			return parse_datetime(event["timestamp"])
	return None

def pretty_print_command(cmd):
	if cmd["type"] == 'CreateCommand':
		return "{0} for template {1}".format(cmd["type"], cmd["templateId"])
	elif cmd["type"] == 'CreateAndExerciseCommand':
		return "{0} exercised choice {1} on template {2}".format(cmd["type"], cmd["choice"], cmd["templateId"])
	elif cmd["type"] == 'ExerciseCommand':
		return "{0} exercised choice {1} on template {2}".format(cmd["type"], cmd["choice"], cmd["templateId"])
	elif cmd["type"] == 'ExerciseByKeyCommand':
		return "{0} exercised choice {1} on template {2} using contact key {3}".format(cmd["type"], cmd["choice"], cmd["templateId"], cmd["contractKey"])

def build_message_dataframe(messages, with_datetime=False):
	if with_datetime:
		data = {'datetime': [datetime_roundup(messages[cmd]["message-arrival-time"]) for cmd in messages], 'message-wait-time': [(messages[cmd]["rule-start-time"] - messages[cmd]["message-arrival-time"]) for cmd in messages], 'rule-evaluation-time': [(messages[cmd]["rule-end-time"] - messages[cmd]["rule-start-time"]) for cmd in messages]}
	else:
		data = {'message-wait-time': [(messages[cmd]["rule-start-time"] - messages[cmd]["message-arrival-time"]) for cmd in messages], 'total-processing-time': [(messages[cmd]["rule-end-time"] - messages[cmd]["message-arrival-time"]) for cmd in messages], 'rule-evaluation-time': [(messages[cmd]["rule-end-time"] - messages[cmd]["rule-start-time"]) for cmd in messages]}		
	return pd.DataFrame(data=data)

def build_completion_arrival_dataframe(received, processed):
	def wait_count(rules):
		return len([ rule for rule in rules if rule not in processed ])
	def processed_count(rules):
		return len([ rule for rule in rules if rule in processed ])
	groups = [ (dt, list(rules)) for (dt, rules) in itertools.groupby(received, key=lambda rule: datetime_roundup(get_completion_arrival_time(rule))) ]
	data = {'datetime': [item[0] for item in groups], 'hidden': [wait_count(item[1]) for item in groups], 'processed': [processed_count(item[1]) for item in groups]}
	return pd.DataFrame(data=data)

def build_transaction_arrival_dataframe(received, processed):
	def wait_count(rules):
		return len([ rule for rule in rules if rule not in processed ])
	def processed_count(rules):
		return len([ rule for rule in rules if rule in processed ])
	groups = [ (dt, list(rules)) for (dt, rules) in itertools.groupby(received, key=lambda rule: datetime_roundup(get_transaction_arrival_time(rule))) ]
	data = {'datetime': [item[0] for item in groups], 'hidden': [wait_count(item[1]) for item in groups], 'processed': [processed_count(item[1]) for item in groups]}
	return pd.DataFrame(data=data)

def build_submission_dataframe(submissions):
	def create_commands(rules):
		return len([ rule for rule in rules for cmd in get_submission_command(rule)[1]["commands"] if cmd["type"] == "CreateCommand" ])
	def exercise_commands(rules):
		return len([ rule for rule in rules for cmd in get_submission_command(rule)[1]["commands"] if cmd["type"] == "ExerciseCommand" ])
	def create_and_exercise_commands(rules):
		return len([ rule for rule in rules for cmd in get_submission_command(rule)[1]["commands"] if cmd["type"] == "CreateAndExerciseCommand" ])
	def exercise_by_key_commands(rules):
		return len([ rule for rule in rules for cmd in get_submission_command(rule)[1]["commands"] if cmd["type"] == "ExerciseByKeyCommand" ])
	groups = [ (dt, list(rules)) for (dt, rules) in itertools.groupby(submissions, key=lambda rule: datetime_roundup(get_submission_command(rule)[1]["timestamp"])) ]
	data = {'datetime': [item[0] for item in groups], 'CreateCommand': [create_commands(item[1]) for item in groups], 'ExerciseCommand': [exercise_commands(item[1]) for item in groups], 'CreateAndExerciseCommand': [create_and_exercise_commands(item[1]) for item in groups], 'ExerciseByKeyCommand': [exercise_by_key_commands(item[1]) for item in groups]}
	return pd.DataFrame(data=data)

def analyse(json_data, id, name):
	trigger = [ child for child in json_data["root"]["children"] if child["span"] == id ][0]
	events = trigger["events"]
	assert set([event["message"] for event in trigger["events"]]) == set(['Subscribing to ledger API transaction source', 'Subscribing to ledger API completion source', 'No heartbeat source configured'])
	children = trigger["children"]
	assert set([child["name"] for child in children]) == set(['trigger.rule.initialization', 'trigger.rule.update'])
	init_events = [child for child in children if child["name"] == 'trigger.rule.initialization']
	update_events = [child for child in children if child["name"] == 'trigger.rule.update']
	print("  - {0} initialisation events".format(len(init_events)))
	for event in init_events:
		if "children" not in event:
			print("  - trigger initState produced no submissions")
		else:
			print("  - trigger initState produced {0} submissions".format(len(event["children"])))
		assert(set([ e["message"] for e in event["events"] ]) == set(['Trigger starting', 'Trigger rule initial state']))
	# TODO: correlated_init_state_submissions = dict([ get_submission_command(rule) for rule in init_events ])
	print("  - {0} rule updates".format(len(update_events)))
	update_submissions = [ rule for rule in update_events if "trigger.batch.submission" in json.dumps(rule) ]
	if len(update_submissions) == 0:
		print("  - trigger updateState produced no submissions")
	else:
		print("  - trigger updateState produced submissions on {0} rule updates".format(len(update_submissions)))
		submission_type = []
		for rule in update_submissions:
			cmds = []
			for cmd in get_submission_command(rule)[1]["commands"]:
				if "contractId" in cmd:
					cmd.pop("contractId")
				cmds.append(cmd)
			submission_type.append(cmds)
		unique_submission_types = []
		for submission in submission_type:
			if submission not in unique_submission_types:
				unique_submission_types.append(submission)
		for submission in unique_submission_types:
			print("    - {0} submissions with commands:".format(len([s for s in submission_type if s == submission])))
			for cmd in submission:
				print("      - {0}".format(pretty_print_command(cmd)))
	df = build_submission_dataframe(update_submissions)
	fig = px.bar(df, x="datetime", y=["CreateCommand", "ExerciseCommand", "CreateAndExerciseCommand", "ExerciseByKeyCommand"], labels=dict(datetime="Datetime (1 minutes buckets)", value="Number of commands"), title="Ledger command submissions for {0}".format(name))
	fig.show()
	correlated_update_submissions = dict([ get_submission_command(rule) for rule in update_submissions ])
	completions_received = [ rule for rule in update_events for event in rule["events"] if event["message"] == 'Completion source' ]
	completions_processed = [ rule for rule in update_events for event in rule["events"] if event["message"] == 'Completion source' and "children" in rule ]
	print("  - Completion messages:")
	print("    - {0} completions received and hidden".format(len(completions_received) - len(completions_processed)))
	print("    - {0} completions processed".format(len(completions_processed)))
	completion_failures = [ rule for rule in completions_processed for event in rule["events"] if event["message"] == 'Completion source' and "details" in event and "message" in event["details"] and "status" in event["details"]["message"] and event["details"]["message"]["status"] != 0 ]
	completion_successes = [ rule for rule in completions_processed for event in rule["events"] if event["message"] == 'Completion source' and "details" in event and "message" in event["details"] and "status" in event["details"]["message"] and event["details"]["message"]["status"] == 0 ]
	print("      - {0} completion failures".format(len(completion_failures)))
	failure_messages = [ get_completion_failure_message(rule) for rule in completion_failures ]
	for failure in set(failure_messages):
		print("        - {0} {1}".format(len([ f for f in failure_messages if f == failure ]), failure))
	print("      - {0} completion successes".format(len(completion_successes)))
	correlated_completion_failures = dict([(get_completion_command_id(rule), {"message": get_completion_failure_message(rule), "message-arrival-time": get_completion_arrival_time(rule), "rule-start-time": get_rule_start_time(rule), "rule-end-time": get_rule_end_time(rule)}) for rule in completion_failures])
	correlated_completion_successes = dict([(get_completion_command_id(rule), {"transactionId": get_completion_transaction_id(rule), "message-arrival-time": get_completion_arrival_time(rule), "rule-start-time": get_rule_start_time(rule), "rule-end-time": get_rule_end_time(rule)}) for rule in completion_successes])
	submissions = [ rule for rule in completions_processed if rule in update_submissions ]
	if len(submissions) == 0:
		print("    - no submissions produced by completion message processing")
	else:
		print("    - {0} completion message rule updates produced submissions".format(len(submissions)))
	print("  - Successful completion message timing statistics:")
	print("")
	print("```")
	df = build_message_dataframe(correlated_completion_successes)
	print(df.describe())
	print("```")
	df = build_message_dataframe(correlated_completion_successes, with_datetime=True)
	fig = px.bar(df, x="datetime", y=["message-wait-time", "rule-evaluation-time"], labels=dict(datetime="Datetime (1 minutes buckets)", value="Time (seconds)"), title="Successful completion message timings for {0}".format(name))
	fig.show()
	print("")
	print("  - Failed completion message timing statistics:")
	print("")
	print("```")
	df = build_message_dataframe(correlated_completion_failures)
	print(df.describe())
	print("```")
	df = build_message_dataframe(correlated_completion_failures, with_datetime=True)
	fig = px.bar(df, x="datetime", y=["message-wait-time", "rule-evaluation-time"], labels=dict(datetime="Datetime (1 minutes buckets)", value="Time (seconds)"), title="Failed completion message timings for {0}".format(name))
	fig.show()
	print("")
	transactions_received = [ rule for rule in update_events for event in rule["events"] if event["message"] == 'Transaction source' ]
	transactions_processed = [ rule for rule in update_events for event in rule["events"] if event["message"] == 'Transaction source' and "children" in rule ]
	print("  - Transaction messages:")
	print("    - {0} transactions received and hidden".format(len(transactions_received) - len(transactions_processed)))
	print("    - {0} transactions processed".format(len(transactions_processed)))
	transaction_events = []
	for rule in transactions_processed:
		transaction = get_transaction_events(rule)
		transaction_events.append({"creates": len(transaction["creates"]), "archives": len(transaction["archives"])})
	correlated_transaction_events = dict([ (get_transaction_command_id(rule), {"transactionId": get_transaction_transaction_id(rule), "events": get_transaction_events(rule), "message-arrival-time": get_transaction_arrival_time(rule), "rule-start-time": get_rule_start_time(rule), "rule-end-time": get_rule_end_time(rule)}) for rule in transactions_processed ])
	for events in [dict(t) for t in {tuple(d.items()) for d in transaction_events}]:
		print("      - {0} transactions had {1} create events and {2} archive events".format(len([ event for event in transaction_events if event == events ]), events["creates"], events["archives"]))
	submissions = [ rule for rule in transactions_processed if rule in update_submissions ]
	if len(submissions) == 0:
		print("    - no submissions produced by transaction message processing")
	else:
		print("    - {0} transaction message rule updates produced submissions".format(len(submissions)))
	print("  - Transaction message timing statistics:")
	print("")
	print("```")
	df = build_message_dataframe(correlated_transaction_events)
	print(df.describe())
	print("```")
	df = build_message_dataframe(correlated_transaction_events, with_datetime=True)
	fig = px.bar(df, x="datetime", y=["message-wait-time", "rule-evaluation-time"], labels=dict(datetime="Datetime (1 minutes buckets)", value="Time (seconds)"), title="Transaction message timings for {0}".format(name))
	fig.show()
	print("")
	df = build_completion_arrival_dataframe(completions_received, completions_processed)
	fig = px.bar(df, x="datetime", y=["processed", "hidden"], labels=dict(datetime="Datetime (1 minutes buckets)", value="Number of messages"), title="Completion message arrivals for {0}".format(name))
	fig.show()
	df = build_transaction_arrival_dataframe(transactions_received, transactions_processed)
	fig = px.bar(df, x="datetime", y=["processed", "hidden"], labels=dict(datetime="Datetime (1 minutes buckets)", value="Number of messages"), title="Transaction message arrivals for {0}".format(name))
	fig.show()
	heartbeats_received = [ rule for rule in update_events for event in rule["events"] if event["message"] == 'Heartbeat source' ]
	heartbeats_processed = [ rule for rule in update_events for event in rule["events"] if event["message"] == 'Heartbeat source' and "children" in rule ]
	print("  - Heartbeat messages:")
	print("    - {0} heartbeats received and hidden".format(len(heartbeats_received) - len(heartbeats_processed)))
	print("    - {0} heartbeats processed".format(len(heartbeats_processed)))
	submissions = [ rule for rule in heartbeats_processed if rule in update_submissions ]
	if len(submissions) == 0:
		print("    - no submissions produced by heartbeat message processing")
	else:
		print("    - {0} heartbeat message rule updates produced submissions".format(len(submissions)))
	in_flight_commands = [ cmd for cmd in correlated_update_submissions if cmd not in correlated_completion_failures and cmd not in correlated_completion_successes and cmd not in correlated_transaction_events ]
	partial_in_flight_commands = [ cmd for cmd in correlated_update_submissions if (cmd in correlated_completion_failures or cmd in correlated_completion_successes) and cmd not in correlated_transaction_events ]
	print("  - {0} commands are currently in-flight".format(len(in_flight_commands) + len(partial_in_flight_commands)))
	print("    - hidden {0} command completions".format(len(in_flight_commands)))
	print("    - hidden {0} transaction events".format(len(partial_in_flight_commands)))
	unexpected_completion_failures = [ cmd for cmd in correlated_completion_failures if cmd not in correlated_update_submissions ]
	if len(unexpected_completion_failures) > 0:
		print("  - Received {0} completion failures with no corresponding command submission".format(len(unexpected_completion_failures)))
		print("    - example command IDs: {0}".format(unexpected_completion_failures[:examples]))
	unexpected_completion_successes = [ cmd for cmd in correlated_completion_successes if cmd not in correlated_update_submissions ]
	if len(unexpected_completion_successes) > 0:
		print("  - Received {0} completion successes with no corresponding command submission".format(len(unexpected_completion_successes)))
		print("    - example command IDs: {0}".format(unexpected_completion_successes[:examples]))
	unexpected_transaction_events = [ cmd for cmd in correlated_transaction_events if cmd not in correlated_update_submissions ]
	if len(unexpected_transaction_events) > 0:
		print("  - Received {0} transaction messages with no corresponding command submission".format(len(unexpected_transaction_events)))
		print("    - example command IDs: {0}".format(unexpected_transaction_events[:examples]))
	impossible_transaction_events = [ cmd for cmd in correlated_transaction_events if cmd in correlated_completion_failures ]
	if len(impossible_transaction_events) > 0:
		print("  - Received {0} transaction messages, but previously we received a command failure!".format(len(impossible_transaction_events)))
		print("    - example command IDs: {0}".format(impossible_transaction_events[:examples]))
	missing_completions = [ cmd for cmd in correlated_transaction_events if cmd not in correlated_completion_failures and cmd not in correlated_completion_successes ]
	if len(missing_completions) > 0:
		print("  - Received {0} transaction messages, but their corresponding completion message is missing".format(len(missing_completions)))
		print("    - example command IDs: {0}".format(missing_completions[:examples]))
	buggy_transactions = [ cmd for cmd in correlated_transaction_events if cmd in correlated_completion_successes and correlated_completion_successes[cmd]["transactionId"] != correlated_transaction_events[cmd]["transactionId"] ]
	if len(buggy_transactions) > 0:
		print("  - Received {0} transaction messages with the transaction ID inconsistent with the corresponding successful completion message transaction ID".format(len(buggy_transactions)))
		print("    - example command IDs: {0}".format(buggy_transactions[:examples]))
	trigger_acs = {}
	duplicate_contracts = {}
	missing_contracts = {}
	for cmd in correlated_transaction_events:
		event = correlated_transaction_events[cmd]["events"]
		for create in event["creates"]:
			if create["templateId"] not in trigger_acs:
				trigger_acs[create["templateId"]] = []
				duplicate_contracts[create["templateId"]] = []
				missing_contracts[create["templateId"]] = []
			if create["contractId"] not in trigger_acs[create["templateId"]]:
				trigger_acs[create["templateId"]].append(create["contractId"])
			else:
				duplicate_contracts[create["templateId"]].append(create["contractId"])
		for archive in event["archives"]:
			if archive["templateId"] not in trigger_acs:
				trigger_acs[archive["templateId"]] = []
				duplicate_contracts[archive["templateId"]] = []
				missing_contracts[archive["templateId"]] = []
			if archive["contractId"] in trigger_acs[archive["templateId"]]:
				trigger_acs[archive["templateId"]].remove(archive["contractId"])
			else:
				missing_contracts[archive["templateId"]].append(archive["contractId"])
	print("  - Trigger view of ACS:")
	for template in trigger_acs:
		print("    - {0}".format(template))
		print("      - has {0} active contracts".format(len(trigger_acs[template])))
		if len(duplicate_contracts[template]) > 0:
			print("      - encountered {0} duplicate contracts".format(len(duplicate_contracts[template])))
			print("        - example contract IDs: {0}".format(duplicate_contracts[template][:examples]))
		if len(missing_contracts[template]) > 0:
			print("      - detected {0} archived contracts with no create event".format(len(missing_contracts[template])))
			print("        - example contract IDs: {0}".format(missing_contracts[template][:examples]))

def main(args):
	with open(os.path.abspath(os.path.expanduser(args.file))) as fd:
		json_data = json.load(fd)

	ProcessFinalTcPdfUploaded = [ child["span"] for child in json_data["root"]["children"] if child["name"] == "trigger.rule" and "D7.DIExecution.Trigger.ProcessFinalTcPdfUploaded" in json.dumps(child) ]
	IssuanceRequestFromDxxl = [ child["span"] for child in json_data["root"]["children"] if child["name"] == "trigger.rule" and "D7.DIExecution.Trigger.IssuanceRequestFromDxxl" in json.dumps(child) ]
	
	for id in ProcessFinalTcPdfUploaded:
		print("# Trigger D7.DIExecution.Trigger.ProcessFinalTcPdfUploaded ({0})".format(id))
		print("  No restarts detected for ProcessFinalTcPdfUploaded") if len(ProcessFinalTcPdfUploaded) == 1 else print("  ProcessFinalTcPdfUploaded has restarted {0} times".format(len(ProcessFinalTcPdfUploaded)))
		analyse(json_data, id, "D7.DIExecution.Trigger.ProcessFinalTcPdfUploaded")
	print("")
	for id in IssuanceRequestFromDxxl:
		print("# Trigger D7.DIExecution.Trigger.IssuanceRequestFromDxxl ({0})".format(id))
		print("  No restarts detected for IssuanceRequestFromDxxl") if len(IssuanceRequestFromDxxl) == 1 else print("  IssuanceRequestFromDxxl has restarted {0} times".format(len(IssuanceRequestFromDxxl)))
		analyse(json_data, id, "D7.DIExecution.Trigger.IssuanceRequestFromDxxl")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyse trigger logging data")
    parser.add_argument("--file", dest="file", metavar="FILE", help="File containing JSON trigger logging data for analysis")
    args = parser.parse_args()
    main(args)
