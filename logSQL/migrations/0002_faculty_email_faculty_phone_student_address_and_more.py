# Generated by Django 4.1.1 on 2022-11-21 05:14

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('logSQL', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='faculty',
            name='email',
            field=models.TextField(default='abc@gmail.com'),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='faculty',
            name='phone',
            field=models.BigIntegerField(default=1234567890),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='student',
            name='address',
            field=models.TextField(default='India'),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='student',
            name='email',
            field=models.TextField(default='abc@xyz.com'),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='student',
            name='phone',
            field=models.BigIntegerField(default=1234567890),
            preserve_default=False,
        ),
    ]
