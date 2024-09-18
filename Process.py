from threading import Lock, Thread

from time import sleep

from Com import Com

class Process(Thread):
    
    def __init__(self,name,nbProcess):
        Thread.__init__(self)

        self.com = Com(nbProcess)
        
        self.nbProcess = self.com.getNbProcess()

        self.myId = self.com.getMyId()
        self.setName(name)


        self.alive = True
        self.start()
    
    def setName(self, name):
        self.name = name

    def criticalAction(self):
        """
        The critical action to do
        :return: None
        """
        if self.com.mailbox.isEmpty():
            print("Catched !")
            self.com.broadcast("J'ai gagné !!!")
        else:
            print(self.com.mailbox.getMsg().getSender(), "a eu le jeton en premier")

    def run(self):
        loop = 0
        while self.alive:
            print(self.getName() + " Loop: " + str(loop))
            sleep(1)

            if self.getName() == "P0":
                self.com.sendTo("j'appelle 2 et je te recontacte après", 1)
                
                self.com.sendToSync("J'ai laissé un message à 2, je le rappellerai après, on se sychronise tous et on attaque la partie ?", 2)
                print("received : ", self.com.recevFromSync(2))
               
                self.com.sendToSync("2 est OK pour jouer, on se synchronise et c'est parti!",1)
                    
                self.com.synchronize()
                    
                self.com.doCriticalAction(self.criticalAction)



            if self.getName() == "P1":
                if  not self.com.mailbox.isEmpty():
                    self.com.mailbox.getMsg()
                    print(self.com.recevFromSync(0))

                    self.com.synchronize()
                    
                    self.criticalAction()
                    
            if self.getName() == "P2":
                print(self.com.recevFromSync(0))
                self.com.sendToSync("OK", 0)

                self.com.synchronize()
                    
                self.com.criticalAction()
                

            loop+=1
        print(self.getName() + " stopped")

    def stop(self):
        self.alive = False
        self.join()
