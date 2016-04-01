package com.asto.dmp.mgtevl.mq

import com.asto.dmp.mgtevl.base.Props
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory, MessageProperties}
import org.apache.spark.Logging

object MQAgent extends Logging {
  private val connection: Connection = getConnection
  private val channel: Channel = connection.createChannel

  def getConnection = {
    val connectionFactory = new ConnectionFactory
    connectionFactory.setHost(Props.get("rabbit_mq_host"))
    connectionFactory.setPort(Props.get("rabbit_mq_port").toInt)
    connectionFactory.setUsername(Props.get("rabbit_mq_username"))
    connectionFactory.setPassword(Props.get("rabbit_mq_password"))
    connectionFactory.newConnection
  }

  def send(message: String) {
    val queueName = Props.get("queue_name_online")
    //以下四行代码的意思我表示不是很清楚，照搬过来的
    channel.queueDeclare(queueName, true, false, false, null)
    channel.exchangeDeclare(queueName, "direct", true)
    channel.queueBind(queueName, queueName, queueName)
    channel.basicPublish(queueName, queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"))
  }

  def close() {
    if (Option(channel).isDefined) channel.close()
    if (Option(connection).isDefined) connection.close()
  }

}
