from django.db import models


class Student(models.Model):
    name = models.TextField()
    age = models.IntegerField()
    address = models.TextField()
    phone = models.BigIntegerField()
    email = models.TextField()


class Faculty(models.Model):
    name = models.TextField()
    age = models.IntegerField()
    subject = models.TextField()
    title = models.TextField()
    phone = models.BigIntegerField()
    email = models.TextField()
