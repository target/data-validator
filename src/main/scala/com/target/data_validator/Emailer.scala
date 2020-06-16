package com.target.data_validator

import java.util.{Date, Properties}

import com.typesafe.scalalogging.LazyLogging
import javax.mail._
import javax.mail.internet._

import scala.util.Try

case class EmailConfig(
  smtpHost: String,
  subject: String,
  from: String,
  to: List[String],
  cc: Option[List[String]] = None,
  bcc: Option[List[String]] = None
) extends EventLog with Substitutable {
  def substituteVariables(dict: VarSubstitution): EmailConfig = {
    EmailConfig(
      getVarSub(smtpHost, "smtpHost", dict),
      getVarSub(subject, "email.subject", dict),
      getVarSub(from, "email.from", dict),
      to.map(getVarSub(_, "email.to", dict)),
      cc.map(_.map(getVarSub(_, "email.cc", dict))),
      bcc.map(_.map(getVarSub(_, "email.bcc", dict)))
    )
  }
}

object Emailer extends LazyLogging {

  def createMessage(smtpHost: String): Message = {
    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)
    val session = Session.getInstance(properties)
    new MimeMessage(session)
  }

  def setMessageRecipients(message: Message, recipients: Seq[String], recipientType: Message.RecipientType): Int = {
    val parsedAddresses = recipients.map(x => Try(InternetAddress.parse(x)))

    val (goodParsed, badParsed) = parsedAddresses.partition(_.isSuccess)

    badParsed.foreach(x => logger.error(s"EmailAddress from $recipientType threw exception $x"))

    val goodAddresses: Array[Address] = goodParsed.flatMap(_.get.toSeq).seq.toArray

    if (!goodAddresses.isEmpty) {
      message.setRecipients(recipientType, goodAddresses)
    }

    goodAddresses.length
  }

  def setFrom(message: Message, from: String): Boolean = {
    try {
      val frm = InternetAddress.parse(from, true)
      message.setFrom(frm.head)
      false
    } catch {
      case ae: AddressException =>
        logger.error(s"setFrom InternetAddress parse failed, $ae")
        true
      case me: MessagingException =>
        logger.error(s"setFrom failed, $me")
        true
    }
  }

  def createEmptyMessage(
    smtpHost: String,
    subject: String,
    from: String,
    to: Seq[String],
    cc: Seq[String],
    bcc: Seq[String]
  ): Option[Message] = {

    logger.debug(s"Creating Message frm: $from to: ${to.mkString(", ")} sub: $subject")
    val message = createMessage(smtpHost)

    val validRecipients = setMessageRecipients(message, cc, Message.RecipientType.CC) +
      setMessageRecipients(message, bcc, Message.RecipientType.BCC) +
      setMessageRecipients(message, to, Message.RecipientType.TO)
    if (validRecipients == 0) {
      logger.error("Must specify at least 1 valid email address in TO, CC, or BCC")
      None
    } else if (setFrom(message, from)) {
      logger.error(s"setFrom($from) failed!")
      None
    } else {
      message.setSentDate(new Date())
      message.setSubject(subject)
      Some(message)
    }
  }

  def createEmptyMessage(ec: EmailConfig): Option[Message] =
    createEmptyMessage(
      ec.smtpHost,
      ec.subject,
      ec.from,
      ec.to,
      ec.cc.getOrElse(Seq.empty),
      ec.bcc.getOrElse(Seq.empty)
    )

  def sendMessage(message: Message, body: String, mime: String): Boolean = {
    message.setContent(body, mime)
    val id = message.hashCode().toHexString
    try {
      logger.info(s"Sending email #$id [${message.getSubject}] to [${message.getAllRecipients.mkString(", ")}]")
      Transport.send(message)
      logger.info(s"Email #$id sent totally successfully.")
      false
    }
    catch {
      case sfe: SendFailedException =>
        logger.warn(s"Failure to send email #$id: ${sfe.getMessage}")
        if (sfe.getValidSentAddresses.nonEmpty) {
          logger.warn(s"Email #$id was sent to [${sfe.getValidSentAddresses.mkString(", ")}]")
        }
        if (sfe.getValidUnsentAddresses.nonEmpty) {
          logger.error(s"Email #$id was not sent to [${sfe.getValidUnsentAddresses.mkString(", ")}]")
        }
        if (sfe.getInvalidAddresses.nonEmpty) {
          logger.error(s"Email #$id has invalid addresses: [${sfe.getInvalidAddresses.mkString(", ")}]")
        }

        true
      case me: MessagingException =>
        logger.error(s"Failure to send email #$id: $me")
        true
    }
  }

  def sendTextMessage(
    smtpHost: String,
    body: String,
    subject: String,
    from: String,
    to: Seq[String],
    cc: Seq[String] = Nil,
    bcc: Seq[String] = Nil
  ): Boolean =
    createEmptyMessage(smtpHost, subject, from, to, cc, bcc) match {
      case None =>
        logger.error("createMessage failed!")
        true
      case Some(message) =>
        sendMessage(message, body, "text/plain; charset=us-ascii")
    }

  def sendTextMessage(emailConfig: EmailConfig, body: String): Boolean = {
    createEmptyMessage(emailConfig) match  {
      case None =>
        logger.error("createMessage failed!")
        true
      case Some(message) =>
        sendMessage(message, body, "text/plain; charset=us-ascii")
    }
  }

  def sendHtmlMessage(
    smtpHost: String,
    body: String,
    subject: String,
    from: String,
    to: Seq[String],
    cc: Seq[String] = Nil,
    bcc: Seq[String] = Nil
  ): Boolean =
    createEmptyMessage(smtpHost, subject, from, to, cc, bcc) match {
      case None =>
        logger.error("createMessage failed!")
        true
      case Some(message) =>
        sendMessage(message, body, "text/html")
    }

  def sendHtmlMessage(config: EmailConfig, body: String): Boolean = {
    createEmptyMessage(config) match {
      case None =>
        logger.error("createMessage failed!")
        true
      case Some(message) =>
        sendMessage(message, body, "text/html")
    }
  }

}
