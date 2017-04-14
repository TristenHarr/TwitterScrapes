from tkinter import *

"""
844275546070528000-3d8mPRa5bCJ7GnIfPeHaIVe3DqxziD0
goiDNMuYrCbmeG5ImSPGCMLbAvSMAS6XTsLsMn0NEjDSf
si0ubj4irm43ZkvjcSMAaqBkt
HIFbnUGmZGa7KA8zVNBY1DTjYfCYR86aGDQdHzRb11TVeaDSrL
"""
class Setup(object):

    def __init__(self):
        self.window = Tk()
        self.width = self.window.winfo_screenwidth()
        self.height = self.window.winfo_screenheight()
        self.window.geometry("600x400")
        self.window.title("Installation")
        self.step = 1
        self.step1 = None
        self.step2 = None
        self.step3 = None

    def run(self):
        self.s1()
        self.window.mainloop()

    def next(self):
        if self.step == 1:
            self.step += 1
            self.s2()
        elif self.step == 2:
            # TODO Connect to API and test authentication is success
            # access_token = self.access_token.get()
            # access_token_secret = self.acc_tok_sec.get()
            # consumer_key = self.consumer_key.get()
            # consumer_secret = self.consumer_secret.get()
            # secrets = open("../InRoute/lockdown.txt", 'w')
            # secrets.write(access_token+"\n")
            # secrets.write(access_token_secret+"\n")
            # secrets.write(consumer_key+"\n")
            # secrets.write(consumer_secret+"\n")
            # secrets.close()
            self.step += 1
            self.s3()
        elif self.step == 3:
            user = self.username.get().strip()
            passw = self.password.get().strip()
            passw2 = self.password_c.get().strip()
            email = self.email.get().strip()
            if passw == passw2:
                if len(user) >= 6 and len(passw) >= 6:
                    files = open("../security/passwords.txt", 'w')
                    files.write(user + "\n")
                    files.write(passw + "\n")
                    files.write(email + "\n")
                    self.s4()
                else:
                    # TODO Add better protection and security, (Maybe authenticate via cloud?)
                    wrong = Label(self.step3, text="Please enter a valid username and password, password should be...")
                    wrong.place(x=200, y=300)
            else:
                wrong = Label(self.step3, text="Please enter a valid username and password")
                wrong.place(x=200, y=300)
        else:
            pass

    def stay(self):
        pass

    def exit(self):
        pass

    def s1(self):
        self.step1 = Frame(self.window)
        self.agree = Button(self.step1, text="Agree", command=self.next)
        self.disagree = Button(self.step1, text="Disagree", command=self.stay)
        self.agree.place(x=400, y=360, width=80)
        self.disagree.place(x=490, y=360, width=80)
        self.step1.place(x=0, y=0, width=600, height=400)

    def s2(self):
        self.step1.destroy()
        self.step2 = Frame(self.window)
        self.goform = Button(self.step2, text="Start", command=self.next)
        self.exits = Button(self.step2, text="Exit", command=self.exit)
        self.access_token_l = Label(self.step2, text="Access Token:")
        self.access_token = Entry(self.step2)
        self.acc_tok_sec_l = Label(self.step2, text="Access Token Secret:")
        self.acc_tok_sec = Entry(self.step2)
        self.consumer_key_l = Label(self.step2, text="Consumer Key")
        self.consumer_key = Entry(self.step2)
        self.consumer_secret_l = Label(self.step2, text="Consumer Secret")
        self.consumer_secret = Entry(self.step2)
        self.access_token_l.place(x=10, y=10, width=120)
        self.access_token.place(x=130, y=10, width=350)
        self.acc_tok_sec_l.place(x=10, y=40, width=120)
        self.acc_tok_sec.place(x=130, y=40, width=350)
        self.consumer_key_l.place(x=10, y=70, width=120)
        self.consumer_key.place(x=130, y=70, width=350)
        self.consumer_secret_l.place(x=10, y=100, width=120)
        self.consumer_secret.place(x=130, y=100, width=350)
        self.exits.place(x=490, y=360, width=80)
        self.goform.place(x=400, y=360, width=80)
        self.step2.place(x=0, y=0, width=600, height=400)

    def s3(self):
        self.step2.destroy()
        self.step3 = Frame(self.window)
        self.window.title("Create Account")
        self.username_l = Label(self.step3, text="Username:")
        self.username = Entry(self.step3)
        self.password_l = Label(self.step3, text="Password:")
        self.password = Entry(self.step3)
        self.password_lc = Label(self.step3, text="Confirm Password:")
        self.password_c = Entry(self.step3)
        self.email_l = Label(self.step3, text="E-mail:")
        self.email = Entry(self.step3)
        self.username_l.place(x=10, y=10, width=120)
        self.username.place(x=130, y=10, width=350)
        self.password_l.place(x=10, y=40, width=120)
        self.password.place(x=130, y=40, width=350)
        self.password_lc.place(x=10, y=70, width=120)
        self.password_c.place(x=130, y=70, width=350)
        self.email_l.place(x=10, y=100, width=120)
        self.email.place(x=130, y=100, width=350)
        self.go = Button(self.step3, text="Confirm", command=self.next)
        self.exits = Button(self.step3, text="Exit", command=self.exit)
        self.go.place(x=400, y=360, width=80)
        self.exits.place(x=490, y=360, width=80)
        self.step3.place(x=0, y=0, width=600, height=400)
        starter = open("../InRoute/limit.txt", "w")
        starter.write("1 COUNT")
        starter.close()
        starter = open("../InRoute/work1.txt", 'w')
        starter.close()
        starter = open("../InRoute/work2.txt", "w")
        starter.close()
        starter = open("../InRoute/work_order.txt", 'w')
        starter.write('1')
        starter.close()
        starter = open("status.txt", 'w')
        starter.write("installed")
        starter.close()

    def s4(self):
        self.step3.destroy()

if __name__ == '__main__':
    file = open("status.txt", 'r')
    if next(file).strip() == "brandnew":
        win = Setup()
        win.run()
    else:
        pass
