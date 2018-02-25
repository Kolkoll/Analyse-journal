import csv
import os
import sys
from datetime import datetime, timedelta


def main_function():
    if len(sys.argv) != 2:
        print("Введено недостаточное количество аргументов, попробуйте снова")
        show_help()
        return
    if os.path.exists(sys.argv[1]) is True and os.path.isfile(sys.argv[1]):
        print_results()


def find_5_users_with_most_number_of_requests():
    amount_of_users_requests = []
    with open(sys.argv[1]) as journal:
        read = csv.DictReader(journal, delimiter=',')
        for user in read:
            if len(user["src_user"]) == 0:
                continue
            if len(amount_of_users_requests) == 0:
                amount_of_users_requests.append([user["src_user"], 1])
                continue
            for i in range(len(amount_of_users_requests)):
                if amount_of_users_requests[i][0] == user["src_user"]:
                    amount_of_users_requests[i][1] += 1
                    break
            else:
                amount_of_users_requests.append([user["src_user"], 1])
        heap_sort(amount_of_users_requests)
        return amount_of_users_requests[0:5]


def find_5_users_who_sent_the_most_data():
    amount_of_users_most_data = []
    with open(sys.argv[1]) as journal:
        read = csv.DictReader(journal, delimiter=',')
        for request in read:
            if len(request["src_user"]) == 0 or len(request["output_byte"]) == 0:
                continue
            if len(amount_of_users_most_data) == 0:
                amount_of_users_most_data.append([request["src_user"], int(request["output_byte"])])
                continue
            for i in range(len(amount_of_users_most_data)):
                if amount_of_users_most_data[i][0] == request["src_user"]:
                    amount_of_users_most_data[i][1] += int(request["output_byte"])
                    break
            else:
                amount_of_users_most_data.append([request["src_user"], int(request["output_byte"])])
        heap_sort(amount_of_users_most_data)
        return amount_of_users_most_data[0:5]


def search_periodic_requests_according_to_field_src_user():
    time_of_requests_foreach_user = dict()
    with open(sys.argv[1]) as journal:
        read = csv.DictReader(journal, delimiter=',')
        for request in read:
            if len(request["src_user"]) == 0:
                continue
            if time_of_requests_foreach_user.get(request["src_user"]) is None:
                time_of_requests_foreach_user.update({request["src_user"]: [datetime(int(request["_time"][0:4]),
                            int(request["_time"][5:7]), int(request["_time"][8:10]), int(request["_time"][11:13]),
                            int(request["_time"][14:16]), int(request["_time"][17:19]))]})
            else:
                time_of_requests_foreach_user[request["src_user"]].append(datetime(int(request["_time"][0:4]),
                            int(request["_time"][5:7]), int(request["_time"][8:10]), int(request["_time"][11:13]),
                            int(request["_time"][14:16]), int(request["_time"][17:19])))
    users_sending_regular_requests = []
    period = 3
    for user in time_of_requests_foreach_user.keys():
        for time in range(len(time_of_requests_foreach_user[user]) - 1):
            if time_of_requests_foreach_user[user][time] - timedelta(0, period) == \
                    time_of_requests_foreach_user[user][time + 1]:
                users_sending_regular_requests.append(user)
                break
            elif time_of_requests_foreach_user[user][time] + timedelta(0, period) == \
                    time_of_requests_foreach_user[user][time + 1]:
                users_sending_regular_requests.append(user)
                break
    if len(users_sending_regular_requests) is None:
        return None
    else:
        return users_sending_regular_requests


def search_periodic_requests_according_to_field_src_ip():
    time_of_requests_foreach_ip = dict()
    with open(sys.argv[1]) as journal:
        read = csv.DictReader(journal, delimiter=',')
        for request in read:
            if len(request["src_ip"]) == 0:
                continue
            if time_of_requests_foreach_ip.get(request["src_ip"]) is None:
                time_of_requests_foreach_ip.update({request["src_ip"]: [datetime(int(request["_time"][0:4]),
                            int(request["_time"][5:7]), int(request["_time"][8:10]), int(request["_time"][11:13]),
                            int(request["_time"][14:16]), int(request["_time"][17:19]))]})
            else:
                time_of_requests_foreach_ip[request["src_ip"]].append(datetime(int(request["_time"][0:4]),
                            int(request["_time"][5:7]), int(request["_time"][8:10]), int(request["_time"][11:13]),
                            int(request["_time"][14:16]), int(request["_time"][17:19])))
    ip_sending_regular_requests = []
    period = 3
    for user in time_of_requests_foreach_ip.keys():
        for time in range(len(time_of_requests_foreach_ip[user]) - 1):
            if time_of_requests_foreach_ip[user][time] - timedelta(0, period) == \
                    time_of_requests_foreach_ip[user][time + 1]:
                ip_sending_regular_requests.append(user)
                break
            elif time_of_requests_foreach_ip[user][time] + timedelta(0, period) == \
                    time_of_requests_foreach_ip[user][time + 1]:
                ip_sending_regular_requests.append(user)
                break
    if len(ip_sending_regular_requests) is None:
        return None
    else:
        return ip_sending_regular_requests


def heap_sort(sequence):
    def swap_items(index1, index2):
        if sequence[index1][1] > sequence[index2][1]:
            sequence[index1], sequence[index2] = sequence[index2], sequence[index1]

    def shift_down(parent, limit):
        while True:
            child = (parent + 1) << 1
            if child < limit:
                if sequence[child][1] > sequence[child - 1][1]:
                    child -= 1
                swap_items(parent, child)
                parent = child
            else:
                break
    # Тело функции heap_sort
    length = len(sequence)
    # Формирование первичной пирамиды
    for index in range((length >> 1) - 1, -1, -1):
        shift_down(index, length)
    # Окончательное упорядочение
    for index in range(length - 1, 0, -1):
        swap_items(index, 0)
        shift_down(0, index)


def show_help():
    print("Для корректной работы программы необходимо указать путь к обрабатываемому журналу")


def print_results():
    with open("results.txt", "w", encoding="utf8") as result:
        result.write("# Поиск 5ти пользователей, сгенерировавших наибольшее количество запросов\n")
        result_1 = find_5_users_with_most_number_of_requests()
        for user in range(len(result_1)):
            result.write(result_1[user][0] + ' ' + str(result_1[user][1]) + '\n')
        result.write("\n# Поиск 5ти пользователей, отправивших наибольшее количество данных\n")
        result_2 = find_5_users_who_sent_the_most_data()
        for user in range(len(result_2)):
            result.write(result_2[user][0] + ' ' + str(result_2[user][1]) + '\n')
        result.write("\n# Поиск регулярных запросов (запросов, выполняющихся периодически), "
                     "по полю src_user\n")
        result_3 = search_periodic_requests_according_to_field_src_user()
        if result_3 is None:
            result.write("Нет периодических запросов по полю src_user")
        else:
            for user in range(len(result_3)):
                result.write(result_3[user] + ' ')
        result.write("\n# Поиск регулярных запросов (запросов, выполняющихся периодически), "
                     "по полю src_ip\n")
        result_4 = search_periodic_requests_according_to_field_src_ip()
        if result_4 is None:
            result.write("Нет периодических запросов по полю src_user")
        else:
            for user in range(len(result_4)):
                result.write(result_4[user] + ' ')


main_function()
