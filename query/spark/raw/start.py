import pexpect
import time
import signal
sp = pexpect.spawn('../spark-shell')
print "starting Spark Shell.."
sp.expect('scala>.*')
print "Spark Shell started"
print "loading init.scala..."
sp.sendline(':load init.scala')
sp.expect('scala>.*')
print "loading init.scala done!"
start = time.time()
print "loading and running loader.scala..."
sp.sendline(':load loader.scala')
sp.expect('scala>.*')
end = time.time()
print "Took",end-start,"time to complete the job"

sp.kill(signal.SIGKILL)
print "Killed spark shell"
