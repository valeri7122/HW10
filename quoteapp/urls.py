from django.urls import path
from . import views

app_name = 'quoteapp'

urlpatterns = [
    path('', views.main, name='main'),
    path('author/<int:author_id>', views.author, name='author'),
    path('quote/', views.quote, name='quote'),
    path('authoradding/', views.authoradding, name='authoradding'),
]
