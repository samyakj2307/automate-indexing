from sqlite3 import IntegrityError

from django.db.models import Q
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .getOperations import main_runner
from .models import Student, Faculty
from .serializers import StudentSerializer, FacultySerializer

"""
Operators Available:
For Text
    - Contains
    - StartsWith
    - EndsWith
    - Exact

For Numbers
    - GreaterThan
    - LessThan
    - GreaterThanOrEqual
    - LessThanOrEqual
    - Equal
    - NotEqual

For Date
    - GreaterThan
    - LessThan
    - GreaterThanOrEqual
    - LessThanOrEqual
    - Equal
    - NotEqual

For Boolean
    - Equal
    - NotEqual

For Array
    - Contains
    - Overlap
    - Any
    - All
    - Length
    - LengthEqual
    - LengthNotEqual
    - LengthGreaterThan
    - LengthLessThan
    - LengthGreaterThanOrEqual
    - LengthLessThanOrEqual

"""


class PerformIndexing(APIView):

    def post(self, request):
        main_runner()
        return Response(status=status.HTTP_200_OK)


class AddStudent(APIView):

    def post(self, request):
        serializer = StudentSerializer(data=request.data)
        if serializer.is_valid():
            try:
                student = serializer.save()
            except IntegrityError as e:
                print(e)
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            if student:
                json = serializer.data
                return Response(json, status=status.HTTP_201_CREATED)
        print(serializer.errors)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class AddFaculty(APIView):

    def post(self, request):
        serializer = FacultySerializer(data=request.data)
        if serializer.is_valid():
            try:
                faculty = serializer.save()
            except IntegrityError as e:
                print(e)
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            if faculty:
                json = serializer.data
                return Response(json, status=status.HTTP_201_CREATED)
        print(serializer.errors)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


def build_student_query(query, parameters):
    if 'name' in parameters:
        if parameters["name"]["operator"] == "Equal":
            query &= Q(name=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "NotEqual":
            query &= ~Q(name=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "Contains":
            query &= Q(name__contains=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "StartsWith":
            query &= Q(name__startswith=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "EndsWith":
            query &= Q(name__endswith=parameters["name"]["value"])

    if 'age' in parameters:
        if parameters["age"]["operator"] == "GreaterThan":
            query &= Q(age__gt=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "LessThan":
            query &= Q(age__lt=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "GreaterThanOrEqual":
            query &= Q(age__gte=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "LessThanOrEqual":
            query &= Q(age__lte=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "Equal":
            query &= Q(age=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "NotEqual":
            query &= ~Q(age=parameters["age"]["value"])

    if 'aadhar_card_no' in parameters:
        if parameters["aadhar_card_no"]["operator"] == "GreaterThan":
            query &= Q(aadhar_card_no__gt=parameters["aadhar_card_no"]["value"])
        elif parameters["aadhar_card_no"]["operator"] == "LessThan":
            query &= Q(aadhar_card_no__lt=parameters["aadhar_card_no"]["value"])
        elif parameters["aadhar_card_no"]["operator"] == "GreaterThanOrEqual":
            query &= Q(aadhar_card_no__gte=parameters["aadhar_card_no"]["value"])
        elif parameters["aadhar_card_no"]["operator"] == "LessThanOrEqual":
            query &= Q(aadhar_card_no__lte=parameters["aadhar_card_no"]["value"])
        elif parameters["aadhar_card_no"]["operator"] == "Equal":
            query &= Q(aadhar_card_no=parameters["aadhar_card_no"]["value"])
        elif parameters["aadhar_card_no"]["operator"] == "NotEqual":
            query &= ~Q(aadhar_card_no=parameters["aadhar_card_no"]["value"])

    if 'email' in parameters:
        if parameters["email"]["operator"] == "Equal":
            query &= Q(email=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "NotEqual":
            query &= ~Q(email=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "Contains":
            query &= Q(email__contains=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "StartsWith":
            query &= Q(email__startswith=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "EndsWith":
            query &= Q(email__endswith=parameters["email"]["value"])

    if 'phone' in parameters:
        if parameters["phone"]["operator"] == "Equal":
            query &= Q(phone=parameters["phone"]["value"])
        elif parameters["phone"]["operator"] == "NotEqual":
            query &= ~Q(phone=parameters["phone"]["value"])

    if 'address' in parameters:
        if parameters["address"]["operator"] == "Equal":
            query &= Q(address=parameters["address"]["value"])
        elif parameters["address"]["operator"] == "NotEqual":
            query &= ~Q(address=parameters["address"]["value"])
        elif parameters["address"]["operator"] == "Contains":
            query &= Q(address__contains=parameters["address"]["value"])
        elif parameters["address"]["operator"] == "StartsWith":
            query &= Q(address__startswith=parameters["address"]["value"])
        elif parameters["address"]["operator"] == "EndsWith":
            query &= Q(address__endswith=parameters["address"]["value"])

    return query


class GetAllStudentList(APIView):

    def get(self, request):

        student_query = Q()
        parameters = request.data["parameters"]

        if parameters:
            student_query = build_faculty_query(student_query, parameters)

        try:
            all_students = Student.objects.filter(student_query)
        except Student.DoesNotExist:
            return Response(status=status.HTTP_404_NOT_FOUND)

        all_serialized_students = StudentSerializer(all_students, many=True).data
        return Response(status=status.HTTP_200_OK, data=all_serialized_students)


def build_faculty_query(query, parameters):
    if 'name' in parameters:
        if parameters["name"]["operator"] == "Equal":
            query &= Q(name=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "NotEqual":
            query &= ~Q(name=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "Contains":
            query &= Q(name__contains=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "StartsWith":
            query &= Q(name__startswith=parameters["name"]["value"])
        elif parameters["name"]["operator"] == "EndsWith":
            query &= Q(name__endswith=parameters["name"]["value"])

    if 'subject' in parameters:
        if parameters["subject"]["operator"] == "Equal":
            query &= Q(subject=parameters["subject"]["value"])
        elif parameters["subject"]["operator"] == "NotEqual":
            query &= ~Q(subject=parameters["subject"]["value"])
        elif parameters["subject"]["operator"] == "Contains":
            query &= Q(subject__contains=parameters["subject"]["value"])
        elif parameters["subject"]["operator"] == "StartsWith":
            query &= Q(subject__startswith=parameters["subject"]["value"])
        elif parameters["subject"]["operator"] == "EndsWith":
            query &= Q(subject__endswith=parameters["subject"]["value"])

    if 'age' in parameters:
        if parameters["age"]["operator"] == "GreaterThan":
            query &= Q(age__gt=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "LessThan":
            query &= Q(age__lt=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "GreaterThanOrEqual":
            query &= Q(age__gte=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "LessThanOrEqual":
            query &= Q(age__lte=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "Equal":
            query &= Q(age=parameters["age"]["value"])
        elif parameters["age"]["operator"] == "NotEqual":
            query &= ~Q(age=parameters["age"]["value"])

    if 'title' in parameters:
        if parameters["title"]["operator"] == "Equal":
            query &= Q(title=parameters["title"]["value"])
        elif parameters["title"]["operator"] == "NotEqual":
            query &= ~Q(title=parameters["title"]["value"])
        elif parameters["title"]["operator"] == "Contains":
            query &= Q(title__contains=parameters["title"]["value"])
        elif parameters["title"]["operator"] == "StartsWith":
            query &= Q(title__startswith=parameters["title"]["value"])
        elif parameters["title"]["operator"] == "EndsWith":
            query &= Q(title__endswith=parameters["title"]["value"])

    if 'email' in parameters:
        if parameters["email"]["operator"] == "Equal":
            query &= Q(email=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "NotEqual":
            query &= ~Q(email=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "Contains":
            query &= Q(email__contains=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "StartsWith":
            query &= Q(email__startswith=parameters["email"]["value"])
        elif parameters["email"]["operator"] == "EndsWith":
            query &= Q(email__endswith=parameters["email"]["value"])

    if 'phone' in parameters:
        if parameters["phone"]["operator"] == "Equal":
            query &= Q(phone=parameters["phone"]["value"])
        elif parameters["phone"]["operator"] == "NotEqual":
            query &= ~Q(phone=parameters["phone"]["value"])
    return query


class GetAllFacultyList(APIView):

    def get(self, request):
        faculty_query = Q()
        parameters = request.data["parameters"]

        if parameters:
            faculty_query = build_faculty_query(faculty_query, parameters)

        try:
            all_faculties = Faculty.objects.filter(faculty_query)
        except Faculty.DoesNotExist:
            return Response(status=status.HTTP_404_NOT_FOUND)

        all_serialized_faculties = FacultySerializer(all_faculties, many=True).data
        return Response(status=status.HTTP_200_OK, data=all_serialized_faculties)


class GetStudentByAge(APIView):
    def get(self, request):
        try:
            all_students = Student.objects.get(age__gte=21, )
            print(all_students)
        except Student.DoesNotExist:
            return Response(status=status.HTTP_404_NOT_FOUND)

        all_serialized_students = StudentSerializer(all_students, many=True).data
        return Response(status=status.HTTP_200_OK, data=all_serialized_students)


class GetFacultyBySubject(APIView):
    def get(self, request):
        try:
            all_faculties = Faculty.objects.get(age__gte=25, subject="Intelligent Database Systems")
            print(all_faculties)
        except Faculty.DoesNotExist:
            return Response(status=status.HTTP_404_NOT_FOUND)

        all_serialized_faculties = FacultySerializer(all_faculties).data
        return Response(status=status.HTTP_200_OK, data=all_serialized_faculties)
