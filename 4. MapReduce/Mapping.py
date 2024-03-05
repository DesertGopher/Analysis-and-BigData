import csv
from datetime import datetime


class MapReduce:
    def __init__(self, files: list) -> None:
        self.files = files
        self.files_amount = len(files)

    def map(self) -> list:
        result = []
        for file in self.files:
            with open(file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                file_result = {}
                for row in reader:
                    caller = row["Вызывающий"]
                    duration = row["Длителоность"]
                    item_dict = {caller: [duration]}
                    if caller not in file_result:
                        file_result.update(item_dict)
                    elif caller in file_result:
                        file_result[caller].append(duration)
            result.append(file_result)
        return result

    def shufflensort(self) -> dict:
        mapping_result = self.map()
        result = {}
        for file in mapping_result:
            for k, v in file.items():
                if k not in result:
                    result[k] = [v]
                elif k in result:
                    result[k].append(v)
        for k, v in result.items():
            result[k] = list(set([x for xs in v for x in xs]))
            if "" in result[k]:
                result[k].remove("")
        return result

    def reduce(self) -> dict:
        shuffle = self.shufflensort()
        for k, v in shuffle.items():
            time_list = [datetime.strptime(x, "%M:%S") for x in v]
            mins = sum(x.minute for x in time_list)
            secs = sum(x.second for x in time_list)
            mins = mins + (secs // 60)
            secs = secs % 60
            shuffle[k] = f"{mins}:{secs}"
        return shuffle

    def get_all_calls_time(self) -> str:
        reduce = self.reduce()
        minutes = 0
        seconds = 0
        for k, v in reduce.items():
            v = datetime.strptime(v, "%M:%S")
            minutes += int(v.minute)
            seconds += int(v.second)
        hours = minutes // 60
        minutes = minutes % 60 + seconds // 60
        seconds = seconds % 60
        result = f"{hours}:{minutes}:{seconds}"
        return result


map_reduce = MapReduce(["list1.csv", "list2.csv", "list3.csv"])
print(map_reduce.get_all_calls_time())

