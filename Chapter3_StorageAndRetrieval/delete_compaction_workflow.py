import os

class LogStructuredDB:
    def __init__(self, filename="data.log"):
        self.filename = filename

    def put(self, key, value):
        with open(self.filename, "a") as f:
            f.write(f"PUT {key} {value}\n")

    def delete(self, key):
        with open(self.filename, "a") as f:
            f.write(f"DELETE {key}\n")

    def compact(self):
        """Merges log segments by removing deleted keys and keeping only the latest values."""
        latest_values = {}
        with open(self.filename, "r") as f:
            for line in f:
                parts = line.strip().split(" ", 2)
                if len(parts) == 3 and parts[0] == "PUT":
                    latest_values[parts[1]] = parts[2]
                elif len(parts) == 2 and parts[0] == "DELETE":
                    if parts[1] in latest_values:
                        del latest_values[parts[1]]

        # Write the compacted log
        with open(self.filename, "w") as f:
            for key, value in latest_values.items():
                f.write(f"PUT {key} {value}\n")

    def show_log(self):
        """Displays the current log file."""
        if os.path.exists(self.filename):
            with open(self.filename, "r") as f:
                print(f.read())

# Testing
db = LogStructuredDB()
db.put("user123", "Sourabh")
db.put("user456", "Tim")
db.put("user123", "Mohit")
db.put("user789", "Apeksha")

print("Before deletion:")
db.show_log()

db.delete("user123")

print("After deletion (Tombstone added):")
db.show_log()

db.compact()

print("After compaction (Data merged, tombstone removed):")
db.show_log()

