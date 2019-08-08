package sergey.dashko

case class Event (good: String, sum: Double, timestamp: Long, category: String, ip: String)
case class EventDataShort(ip: String, sum: Double)
