from flask import json, request

from arlo.web.authentication import ResourceWithAuth
from classbot.book import book_class_with_info, plan_booking, cancel_booked_class, cancel_scheduled_class
from classbot.reservations import get_calendar_upcoming
from classbot.schedule import get_calendar_classes
from classbot.users import get_users, get_credits
from classbot.venues import get_user_bookmarks


class GetClassbotVenues(ResourceWithAuth):
    @staticmethod
    def get():
        name = request.args.get('name')
        venues = get_user_bookmarks(name)
        return json.loads(venues)


class GetClassbotUsers(ResourceWithAuth):
    @staticmethod
    def get():
        return get_users()


class LoginClasspassUser(ResourceWithAuth):
    @staticmethod
    def get():
        name = request.args.get('name')
        return get_credits(name)


class GetClasspassCalendar(ResourceWithAuth):
    @staticmethod
    def get():
        venue_id = request.args.get('venue_id')
        name = request.args.get('name')
        long = request.args.get('view_more') == 'true'
        return json.loads(json.dumps(get_calendar_classes(name, venue_id, long=long)))


class GetClasspassUpcoming(ResourceWithAuth):
    @staticmethod
    def get():
        name = request.args.get('name')
        mobile = request.args.get('mobile')
        return json.loads(json.dumps(get_calendar_upcoming(name, mobile)))


class ClassPassBookNow(ResourceWithAuth):
    @staticmethod
    def post():
        class_id = request.args.get('class_id')
        class_credits = request.args.get('class_credits')
        user = request.args.get('user')
        return book_class_with_info(user, class_id, class_credits)


class ClassPassBookLater(ResourceWithAuth):
    @staticmethod
    def post():
        classe_id = request.args.get('class_id')
        user = request.args.get('user')
        class_date = request.args.get('class_date')
        plan_booking(classe_id, user, class_date)
        print('book later', classe_id, user)
        return True


class ClassPassCancelBooked(ResourceWithAuth):
    @staticmethod
    def post():
        classe_id = request.args.get('class_id')
        user = request.args.get('user')
        return cancel_booked_class(user, classe_id).status_code == 200


class ClassPassCancelScheduled(ResourceWithAuth):
    @staticmethod
    def post():
        classe_id = request.args.get('class_id')
        user = request.args.get('user')
        return cancel_scheduled_class(user, classe_id)
