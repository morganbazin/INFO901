from Message import Message


class Mailbox:

    def __init__(self):
        self.messages = []

    def isEmpty(self) -> bool:

        return len(self.messages) == 0

    def addMessage(self, msg: Message):
 
        self.messages.append(msg)

    def getMsg(self) -> Message:

        return self.messages.pop(0)