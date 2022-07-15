from flask_login import UserMixin

class User(UserMixin):
    def __init__(self, id_):
        self.id = id_
        self.user_league = "League not set"
        self.user_franchise = "Franchise not set"

    @staticmethod
    def get(user_id): 
        emailList = ["travisdharry@gmail.com"]
        # Check to see if user is in emailList
        if user_id in emailList:
            user = User(user_id)
            return user
        else:
            return None
    
    def change_user_league(self, input_league):
        self.user_league = input_league
