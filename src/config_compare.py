import yaml

# ---- Specify your two YAML files here ----
FILE_1 = "configs/jobs_050726.yml"
FILE_2 = "configs/jobs_051426.yml"


def load_yaml_as_dict(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        data = yaml.safe_load(f)

    # Your YAML has top-level keys: defaults, jobs
    jobs = data.get("jobs", [])

    result = {}
    for item in jobs:
        name = item.get("name")
        cohort_id = item.get("inputs", {}).get("cohort_id")

        if name:
            result[name] = cohort_id

    return result


def main():
    data1 = load_yaml_as_dict(FILE_1)
    data2 = load_yaml_as_dict(FILE_2)

    common_names = sorted(set(data1.keys()) & set(data2.keys()))

    if not common_names:
        print("No matching names found.")
        return

    print("Matching items and cohort_ids:\n")

    for name in common_names:
        print(f"Name: {name}")
        print(f"  {FILE_1} cohort_id: {data1.get(name)}")
        print(f"  {FILE_2} cohort_id: {data2.get(name)}")
        print()

if __name__ == "__main__":
    main()