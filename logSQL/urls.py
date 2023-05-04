from django.urls import path

from . import views

urlpatterns = [
    path('performIndexing/', views.PerformIndexing.as_view()),
    path('getStudent', views.GetAllStudentList.as_view()),
    path('getStudentByAge', views.GetStudentByAge.as_view()),
    path('addStudent', views.AddStudent.as_view()),

    path('getFaculty', views.GetAllFacultyList.as_view()),
    path('getFacultyBySubject', views.GetFacultyBySubject.as_view()),
    path('addFaculty', views.AddFaculty.as_view()),
]
