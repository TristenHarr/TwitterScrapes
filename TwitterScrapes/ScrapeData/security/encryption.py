

class Auth(object):

    def __init__(self, username, password):
        self.password = password
        self.username = username

    def _check_password(self, username, password):
        check = open('passwords.txt', 'r')
        user = check.readline().strip()
        passw = check.readline().strip()
        email = check.readline().strip()
        print(username, password)
        check.close()
        if user == username and passw == password:
            print("Authenticated")
            return True
        else:
            return False

    def set_password(self, username, current_password):
        if self._check_password(username, current_password):
            new_pass = str(input("New password?").strip())
            confirm = input("Change password to {}   y/n".format(new_pass)) == 'y'
            if confirm:
                my_file = open('passwords.txt', 'w')
                my_file.write(username+'\n')
                my_file.write(new_pass+'\n')




