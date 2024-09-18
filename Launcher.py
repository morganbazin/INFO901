from threading import Thread
from time import sleep
from typing import List

from Process import Process


def launch(nbProcessToCreate: int, runningTime: int):

    def createProcess(x: int):
        processes.append(Process("P" + str(x), nbProcessToCreate))

    processes = []

    processes_launches: List[Thread] = []

    for i in range(nbProcessToCreate):
        processes_launches.append(Thread(target=createProcess, args=(i,)))

    for p in processes_launches:
        p.start()
    for p in processes_launches:
        p.join()

    sleep(runningTime)

    for p in processes:
        p.stop()



if __name__ == '__main__':
    launch(3, 5)
