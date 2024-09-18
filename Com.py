import random
from time import sleep
from typing import Callable, List

from Mailbox import Mailbox

from pyeventbus3.pyeventbus3 import PyBus, subscribe, Mode

from Message import *

class Com:

    def __init__(self, nbProcess):
        self.total_processes = nbProcess
        self.process_identifier = None
        self.initial_id_list = []

        self.active_process_list = []
        self.potential_active_list = []

        PyBus.Instance().register(self, self)
        sleep(1)

        self.message_storage = Mailbox()
        self.clock_time = 0

        self.sync_count = 0
        self.sync_in_progress = False

        self.token_status = TokenState.Null
        self.current_token = None

        self.blocked_state = False
        self.waiting_for_process = []
        self.received_data = None

        self.is_active = True
        if self.getMyId() == self.total_processes - 1:
            self.current_token = random.randint(0, 10000 * (self.total_processes - 1))
            self.sendToken()

    def getNbProcess(self) -> int:

        return self.total_processes

    def getMyId(self) -> int:

        if self.process_identifier is None:
            self.initMyId()
        return self.process_identifier

    def initMyId(self):
        random_id = random.randint(0, 10000 * (self.total_processes - 1))
        print(self, ["Generated random process id:", random_id])
        self.sendMessage(InitIdMessage(random_id))
        sleep(2)
        if len(set(self.initial_id_list)) != self.total_processes:
            print("ID conflict, retrying initialization")
            self.initial_id_list = []
            return self.initMyId()
        self.initial_id_list.sort()
        self.process_identifier = self.initial_id_list.index(random_id)
        print("Assigned process id:", self.process_identifier, "ID list:", self.initial_id_list, "Randomly generated ID:", random_id)

    @subscribe(threadMode=Mode.PARALLEL, onEvent=InitIdMessage)
    def onReceiveInitIdMessage(self, message: InitIdMessage):

        print("Received InitIdMessage with value:", message.getObject())
        self.initial_id_list.append(message.getObject())

    def sendMessage(self, message: Message):

        if not message.is_system:
            self.incClock()
            message.horloge = self.clock_time
        print("Sending message:", message)
        PyBus.Instance().post(message)

    def sendTo(self, obj: any, com_to: int):

        self.sendMessage(MessageTo(obj, self.getMyId(), com_to))

    @subscribe(threadMode=Mode.PARALLEL, onEvent=MessageTo)
    def onReceive(self, message: MessageTo):

        if message.to_id != self.getMyId() or type(message) in [MessageToSync, Token, AcknowledgementMessage]:
            return
        if not message.is_system:
            self.clock_time = max(self.clock_time, message.horloge) + 1
        print("Received MessageTo from process", message.from_id, ":", message.getObject())
        self.message_storage.addMessage(message)

    def sendToSync(self, obj: any, com_to: int):

        self.waiting_for_process = com_to
        self.sendMessage(MessageToSync(obj, self.getMyId(), com_to))
        while com_to == self.waiting_for_process:
            if not self.is_active:
                return

    def recevFromSync(self, com_from: int) -> any:

        self.waiting_for_process = com_from
        while com_from == self.waiting_for_process:
            if not self.is_active:
                return
        received_value = self.received_data
        self.received_data = None
        return received_value

    @subscribe(threadMode=Mode.PARALLEL, onEvent=MessageToSync)
    def onReceiveSync(self, message: MessageToSync):

        if message.to_id != self.getMyId():
            return
        if not message.is_system:
            self.clock_time = max(self.clock_time, message.horloge) + 1
        while message.from_id != self.waiting_for_process:
            if not self.is_active:
                return
        self.waiting_for_process = -1
        self.received_data = message.getObject()
        self.sendMessage(AcknowledgementMessage(self.getMyId(), message.from_id))

    def broadcastSync(self, com_from: int, obj: any = None) -> any:

        if self.getMyId() == com_from:
            print("Broadcasting synchronously:", obj)
            for i in range(self.total_processes):
                if i != self.getMyId():
                    self.sendToSync(obj, i)
        else:
            return self.recevFromSync(com_from)

    @subscribe(threadMode=Mode.PARALLEL, onEvent=AcknowledgementMessage)
    def onAckSync(self, event: AcknowledgementMessage):

        if self.getMyId() == event.to_id:
            print("Received AcknowledgementMessage from process", event.from_id)
            self.waiting_for_process = -1

    def synchronize(self):

        self.sync_in_progress = True
        print("Synchronization started")
        while self.sync_in_progress:
            sleep(0.1)
            print("Synchronization ongoing")
            if not self.is_active:
                return
        while self.sync_count != 0:
            sleep(0.1)
            print("Synchronization finalizing")
            if not self.is_active:
                return
        print("Synchronization complete")

    def requestSC(self):

        print("Requesting critical section")
        self.token_status = TokenState.Requested
        while self.token_status == TokenState.Requested:
            if not self.is_active:
                return
        print("Critical section acquired")

    def broadcast(self, obj: any):

        self.sendMessage(BroadcastMessage(obj, self.getMyId()))

    @subscribe(threadMode=Mode.PARALLEL, onEvent=BroadcastMessage)
    def onBroadcast(self, message: BroadcastMessage):

        if message.from_id == self.getMyId():
            return
        print("Received broadcast message from process", message.from_id, ":", message.getObject())
        if not message.is_system:
            self.clock_time = max(self.clock_time, message.horloge) + 1
        self.message_storage.addMessage(message)

    def sendToken(self):

        if self.current_token is None:
            return
        sleep(0.1)
        self.sendMessage(Token(self.getMyId(), (self.getMyId() + 1) % self.total_processes, self.sync_count, self.current_token))
        self.current_token = None

    def releaseSC(self):

        print("Releasing critical section")
        if self.token_status == TokenState.SC:
            self.token_status = TokenState.Release
        self.sendToken()
        self.token_status = TokenState.Null
        print("Critical section released")

    def incClock(self):

        self.clock_time += 1

    def getClock(self) -> int:

        return self.clock_time

    def stop(self):

        self.is_active = False

    @subscribe(threadMode=Mode.PARALLEL, onEvent=Token)
    def onToken(self, event: Token):

        if event.to_id != self.getMyId() or not self.is_active:
            return
        print("Received token from process", event.from_id)
        self.current_token = event.currentTokenId
        self.sync_count = event.nbSync + int(self.sync_in_progress) % self.total_processes
        self.sync_in_progress = False
        if self.token_status == TokenState.Requested:
            self.token_status = TokenState.SC
        else:
            self.sendToken()

    def doCriticalAction(self, funcToCall: Callable, *args: List[any]) -> any:

        self.requestSC()
        result = None
        if self.is_active:
            if args is None:
                result = funcToCall()
            else:
                result = funcToCall(*args)
            self.releaseSC()
        return result
