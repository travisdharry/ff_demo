from flask_login import UserMixin

class User(UserMixin):
    def __init__(self, id_):
        self.id = id_

    @staticmethod
    def get(user_id): 
        emailList = ["travisdharry@gmail.com"]
        # Check to see if user is in emailList
        if user_id in emailList:
            user = User(user_id)
            return user
        else:
            return None
    
