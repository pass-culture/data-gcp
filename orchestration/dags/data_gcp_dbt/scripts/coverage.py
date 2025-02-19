import json
import sys


class BadgeConfig:
    """Configuration for badge thresholds and icons."""

    FIRST_THRESHOLD = 10.0
    FIRST_ICON = "🥇"
    SECOND_THRESHOLD = 9.0
    SECOND_ICON = "🥈"
    THIRD_THRESHOLD = 2.0
    THIRD_ICON = "🥉"
    WIP_ICON = "🏗️"

    @classmethod
    def get_badge(cls, score):
        """Return the badge icon based on the score."""
        if score >= cls.FIRST_THRESHOLD:
            return cls.FIRST_ICON
        elif score >= cls.SECOND_THRESHOLD:
            return cls.SECOND_ICON
        elif score >= cls.THIRD_THRESHOLD:
            return cls.THIRD_ICON
        else:
            return cls.WIP_ICON


def load_json(file_path):
    """Load JSON data from a file."""
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading JSON data: {e}")
        sys.exit(1)


def print_coverage_summary(data):
    """Print the overall documentation coverage."""
    overall_coverage = data["coverage"] * 100
    total_covered = data["covered"]
    total_columns = data["total"]
    badge = BadgeConfig.get_badge(overall_coverage)

    print(
        f"Coverage for documentation: {overall_coverage:.2f}% ({total_covered} / {total_columns}) {badge}"
    )


def print_table_coverage_below_threshold(data, coverage_threshold):
    """Print tables and columns with coverage below a given threshold."""
    print(f"\nTables with coverage lower than {coverage_threshold * 100:.0f}%:")
    for table in data["tables"]:
        table_name = table["name"]
        table_coverage = table["coverage"]
        table_covered = table["covered"]
        table_total = table["total"]

        if table_coverage < coverage_threshold:
            badge = BadgeConfig.get_badge(table_coverage * 100)
            print(
                f"Table: {table_name} (coverage: {table_coverage * 100:.2f}%, {table_covered} / {table_total}) {badge}"
            )
            for column in table["columns"]:
                column_name = column["name"]
                column_coverage = column["coverage"]
                if column_coverage < coverage_threshold:
                    print(f"      Column: {column_name}")


def main():
    # Get arguments from the command line
    if len(sys.argv) < 3:
        print("Usage: python script.py <json_file> <coverage_threshold>")
        sys.exit(1)

    json_file = sys.argv[1]
    coverage_threshold = float(sys.argv[2])

    # Load the JSON data
    data = load_json(json_file)

    # Print overall documentation coverage
    print_coverage_summary(data)

    # Print tables with coverage lower than the threshold
    print_table_coverage_below_threshold(data, coverage_threshold)


if __name__ == "__main__":
    main()
