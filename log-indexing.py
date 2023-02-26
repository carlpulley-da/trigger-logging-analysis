import argparse
import json
import networkx
import os.path
import re

# TODO: take into account wf graph nodes (c.f. shared nodes) with more than 1 parent

class TriggerLogging:
    def __init__(self):
        self.graph = networkx.DiGraph()

    def add(self, log_entry):
        if "trigger" not in log_entry:
            return
        trigger = log_entry["trigger"]
        span_id = trigger["span"]["id"] or "root"
        if "parent" in trigger["span"]:
            if isinstance(trigger["span"]["parent"], list):
                span_parent = json.dumps(trigger["span"]["parent"])
            else:
                span_parent = trigger["span"]["parent"] or "root"
        else:
            span_parent = "root"
        span_name = trigger["span"]["name"]
        trigger.pop("span")

        self.graph.add_node(span_id, name=span_name)
        self.graph.add_node(span_parent)
        self.graph.add_edge(span_parent, span_id)
        if "events" not in self.graph.nodes[span_id]:
            self.graph.nodes[span_id]["events"] = []
        event = {
            "timestamp": log_entry["@timestamp"],
            "message": log_entry["message"],
            "level": log_entry["level"],
            "details": trigger,
        }
        uuids = self._extract_uuids(log_entry["message"]) + (self._extract_uuids(trigger["message"]) if "message" in trigger and isinstance(trigger["message"], str) else [])
        ids = self._extract_ids(log_entry["message"]) + (self._extract_ids(trigger["message"]) if "message" in trigger and isinstance(trigger["message"], str) else [])
        if len(uuids) + len(ids) > 0:
            event["labels"] = list(set(uuids + ids))
        self.graph.nodes[span_id]["events"].append(event)
        self.graph.nodes[span_id]["span"] = span_id

    def display(self):
        graph = {}
        for root in self._find_roots():
            graph[root] = self._display_graph(root)
        print(json.dumps(graph, indent=2, sort_keys=True))

    def _extract_uuids(self, text):
        return re.findall("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", text)

    def _extract_ids(self, text):
        return re.findall("[0-9a-fA-F]{64,}", text)

    def _find_roots(self):
        return [ root for root in self.graph.nodes if self.graph.in_degree(root) == 0 ]

    def _display_graph(self, root):
        def add_children(node):
            neighbours = self.graph[node]
            if len(neighbours) == 0:
                return []
            collect_children = []
            for child in neighbours:
                nodes = dict(self.graph.nodes[child].items())
                new_child = add_children(child)
                if new_child:
                    nodes["children"] = new_child
                collect_children.append(nodes)
            return collect_children

        data = dict(self.graph.nodes[root].items())
        data["children"] = add_children(root)
        return data

def main(args):
    logging = TriggerLogging()
    with open(os.path.abspath(os.path.expanduser(args.file))) as fd:
        json_data = json.load(fd)
        for entry in json_data:
            logging.add(entry)
    logging.display()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Format and display trigger logging")
    parser.add_argument("--file", dest="file", metavar="FILE", help="File containing JSON trigger logging")
    args = parser.parse_args()
    main(args)
