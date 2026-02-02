"""
URL configuration for config project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path
from config.views import health_check
from metrics.views import import_csv
from metrics.views import quality_report
from metrics.views import daily_metrics
from metrics.views import daily_metrics
from metrics.views import metrics_summary

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/health/", health_check),
    path("api/import/", import_csv),
    path("api/quality/", quality_report),
    path("api/metrics/daily/", daily_metrics),
    path("api/metrics/summary/", metrics_summary),
]

