# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from decimal import Decimal
import urllib.request
from datetime import datetime

from django.http import HttpResponse
from django.contrib.auth.models import User
from concurrent.futures import ThreadPoolExecutor

import logging
# Get an instance of a logger
logger = logging.getLogger(__name__)

# Create your views here.
from rest_framework.permissions import AllowAny
from rest_framework.decorators import (api_view, authentication_classes,  action,permission_classes)
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from rest_framework import status

from restapi.models import (Expenses, Groups, Category)
from restapi.serializers import (UserSerializer, CategorySerializer,  GroupSerializer,  ExpensesSerializer, UserExpense)
from restapi.custom_exception import (UnauthorizedUserException, BadRequestException)

from constants import MAX_TIME_FOR_READING

def index(_request):
    logging.info("index: Function executed successfully")
    return HttpResponse("Hello, world. You're at Rest.")


@api_view(['POST'])
def logout(request):
    request.user.auth_token.delete()
    looging.info("logout: auth token deleted and user logged out")
    return Response(status=status.HTTP_204_NO_CONTENT)

def get_user_ids(body,type):
    user_ids = []
    if body.get(type, None) is not None and body[type].get('user_ids', None) is not None:
            user_ids = body[type]['user_ids']
            for user_id in user_ids:
                if not User.objects.filter(id=user_id).exists():
                    logging.info("get_user_ids: Bad request exception")
                    raise BadRequestException()
    return user_ids

@api_view(['GET'])
def balance(request):
    user = request.user
    expenses = Expenses.objects.filter(users__in=user.expenses.all())
    final_balance = {}
    for expense in expenses:
        expense_balances = normalize(expense)
        for eb in expense_balances:
            from_user = eb['from_user']
            to_user = eb['to_user']
            if from_user == user.id:
                final_balance[to_user] = final_balance.get(to_user, 0) - eb['amount']
            if to_user == user.id:
                final_balance[from_user] = final_balance.get(from_user, 0) + eb['amount']
    final_balance = {k: v for k, v in final_balance.items() if v != 0}

    response = [{"user": k, "amount": int(v)} for k, v in final_balance.items()]
    logging.info("balance: final balance returned")
    return Response(response, status=status.HTTP_200_OK)


def normalize(expense):
    user_balances = expense.users.all()
    dues = {}
    for user_balance in user_balances:
        dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                  - user_balance.amount_owed
    dues = [(k, v) for k, v in sorted(dues.items(), key=lambda item: item[1])]
    start = 0
    end = len(dues) - 1
    balances = []
    while start < end:
        amount = min(abs(dues[start][1]), abs(dues[end][1]))
        user_balance = {"from_user": dues[start][0].id, "to_user": dues[end][0].id, "amount": amount}
        balances.append(user_balance)
        dues[start] = (dues[start][0], dues[start][1] + amount)
        dues[end] = (dues[end][0], dues[end][1] - amount)
        if dues[start][1] == 0:
            start += 1
        else:
            end -= 1
    return balances


class UserViewSet(ModelViewSet):
    QuerySet = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = (AllowAny,)


class CategoryViewSet(ModelViewSet):
    QuerySet = Category.objects.all()
    serializer_class = CategorySerializer
    http_method_names = ['get', 'post']


class GroupViewSet(ModelViewSet):
    QuerySet = Groups.objects.all()
    serializer_class = GroupSerializer

    def getQuerySet(self):
        user = self.request.user
        groups = user.members.all()
        if self.request.query_params.get('q', None) is not None:
            groups = groups.filter(name__icontains=self.request.query_params.get('q', None))
            logging.info(f"GROUP_VIEW_SET: getQuerySet: Returned users with name containing{self.request.query_params.get('q', None)}")
        return groups

    def create(self, request, *args, **kwargs):
        user = self.request.user
        data = self.request.data
        group = Groups(**data)
        group.save()
        group.members.add(user)
        serializer = self.get_serializer(group)
        logging.info("GROUP_VIEW_SET: create: user added to the group")
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    @action(methods=['put'], detail=True)
    def members(self, request, pk=None):
        group = Groups.objects.get(id=pk)
        if group not in self.getQuerySet():
            raise UnauthorizedUserException()
        body = request.data
        added_ids=get_user_ids(body,'add')
        group.members.add(*added_ids)
        logging.info("GROUP_VIEW_SET: members: Users added")
        
        removed_ids=get_user_ids(body,'remove')
        group.members.remove(*user_id)
        logging.info("GROUP_VIEW_SET: members: Users removed")
        group.save()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(methods=['get'], detail=True)
    def expenses(self, _request, pk=None):
        group = Groups.objects.get(id=pk)
        if group not in self.getQuerySet():
            raise UnauthorizedUserException()
        expenses = group.expenses_set
        serializer = ExpensesSerializer(expenses, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(methods=['get'], detail=True)
    def balances(self, _request, pk=None):
        group = Groups.objects.get(id=pk)
        if group not in self.getQuerySet():
            raise UnauthorizedUserException()
        expenses = Expenses.objects.filter(group=group)
        dues = {}
        for expense in expenses:
            user_balances = UserExpense.objects.filter(expense=expense)
            for user_balance in user_balances:
                dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                          - user_balance.amount_owed
        dues = [(k, v) for k, v in sorted(dues.items(), key=lambda item: item[1])]
        start = 0
        end = len(dues) - 1
        balances = []
        while start < end:
            amount = min(abs(dues[start][1]), abs(dues[end][1]))
            amount = Decimal(amount).quantize(Decimal(10)**-2)
            user_balance = {"from_user": dues[start][0].id, "to_user": dues[end][0].id, "amount": str(amount)}
            balances.append(user_balance)
            dues[start] = (dues[start][0], dues[start][1] + amount)
            dues[end] = (dues[end][0], dues[end][1] - amount)
            if dues[start][1] == 0:
                start += 1
            else:
                end -= 1
        logging.info("GROUP_VIEW_SET: baances: Calculated balances")
        return Response(balances, status=status.HTTP_200_OK)


class ExpensesViewSet(ModelViewSet):
    querySet = Expenses.objects.all()
    serializer_class = ExpensesSerializer

    def getQuerySet(self):
        user = self.request.user
        if self.request.query_params.get('q', None) is not None:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())\
                .filter(description__icontains=self.request.query_params.get('q', None))
        else:
            expenses = Expenses.objects.filter(users__in=user.expenses.all())
        return expenses

@api_view(['post'])
@authentication_classes([])
@permission_classes([])
def logProcessor(request):
    data = request.data
    num_threads = data['parallelFileProcessingCount']
    log_files = data['logFiles']
    if num_threads <= 0 or num_threads > 30:
        return Response({"status": "failure", "reason": "Parallel Processing Count out of expected bounds"},
                        status=status.HTTP_400_BAD_REQUEST)
    if len(log_files) == 0:
        return Response({"status": "failure", "reason": "No log files provided in request"},
                        status=status.HTTP_400_BAD_REQUEST)
    logs = multiThreadedReader(urls=data['logFiles'], num_threads=data['parallelFileProcessingCount'])
    sorted_logs = sort_by_time_stamp(logs)
    cleaned = transform(sorted_logs)
    data = aggregate(cleaned)
    response = response_format(data)
    return Response({"response":response}, status=status.HTTP_200_OK)

def sort_by_time_stamp(logs):
    data = []
    for log in logs:
        data.append(log.split(" "))
    # print(data)
    data = sorted(data, key=lambda elem: elem[1])
    logging.info("sort_by_time_stamp: Data sorted according to timestamp")
    return data

def response_format(raw_data):
    response = []
    for timestamp, data in raw_data.items():
        entry = {'timestamp': timestamp}
        logs = []
        data = {k: data[k] for k in sorted(data.keys())}
        for exception, count in data.items():
            logs.append({'exception': exception, 'count': count})
        entry['logs'] = logs
        response.append(entry)
    return response

def aggregate(cleaned_logs):
    data = {}
    for log in cleaned_logs:
        [key, text] = log
        value = data.get(key, {})
        value[text] = value.get(text, 0)+1
        data[key] = value
    return data


def transform(logs):
    result = []
    for log in logs:
        [_, timestamp, text] = log
        text = text.rstrip()
        timestamp = datetime.utcfromtimestamp(int(int(timestamp)/1000))
        hours, minutes = timestamp.hour, timestamp.minute
        key = ''

        if minutes >= 45:
            if hours == 23:
                key = "{:02d}:45-00:00".format(hours)
            else:
                key = "{:02d}:45-{:02d}:00".format(hours, hours+1)
        elif minutes >= 30:
            key = "{:02d}:30-{:02d}:45".format(hours, hours)
        elif minutes >= 15:
            key = "{:02d}:15-{:02d}:30".format(hours, hours)
        else:
            key = "{:02d}:00-{:02d}:15".format(hours, hours)

        result.append([key, text])
        print(key)

    return result


def reader(url, timeout):
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()


def multiThreadedReader(urls, num_threads):
    """
        Read multiple files through HTTP
    """
    if num_threads<=0 or num_threads>30:
        logging.error('multiThreadedReader: Parallel Processing Count out of expected bounds')
        raise Exception("Parallel Processing Count out of expected bounds")
        
    result = []
    with ThreadPoolExecutor(max_workers=num_threads) as executors:
        futures = {executor.submit(reader, url, MAX_TIME_FOR_READING): url for url in urls}
        for future in concurrent.futures.as_completed(futures):
            data = future.result()
            data = data.decode('utf-8')
            result.extend(data.split("\n"))
        result = sorted(result, key = lambda elem:elem[1])
    return result