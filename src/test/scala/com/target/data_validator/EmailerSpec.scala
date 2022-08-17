package com.target.data_validator

import java.util.Random
import javax.mail.Message
import javax.mail.internet.{InternetAddress, MimeMessage}

import org.scalatest.funspec.AnyFunSpec

class EmailerSpec extends AnyFunSpec {

  private val doug_work = "doug@example.com"
  private val doug_home = "doug@example.net"
  private val mailingList = List(doug_work, doug_home)
  private val collin_email = "collin@example.com"
  private val patrick_email = "patrick@example.com"

  private def mkMsg() = Emailer.createMessage("smtp.example.com")

  describe("An Emailer") {

    describe("createMessage") {

      it("should create MimeMessage") {
        val msg = mkMsg()
        assert(msg.isInstanceOf[MimeMessage])
      }

    }

    describe("setFrom()") {

      it("should set from address properly") {
        val msg = mkMsg()
        assert(!Emailer.setFrom(msg, doug_work))
        val frmAddress = msg.getFrom.asInstanceOf[Array[InternetAddress]]
        assert(frmAddress.length == 1)
        assert(frmAddress(0).getAddress == doug_work)
      }

      it("should return true with bad from address") {
        val msg = mkMsg()
        val badEmail = "collin@@bad.com"
        assert(Emailer.setFrom(msg, badEmail))
      }

    }

    describe("setMessageRecipients()") {

      it("should set 'to' correctly") {
        val msg = mkMsg()
        Emailer.setMessageRecipients(msg, mailingList, Message.RecipientType.TO)
        val toAddresses = msg.getRecipients(Message.RecipientType.TO).asInstanceOf[Array[InternetAddress]]
        assert(toAddresses.length == 2)
        assert(toAddresses(0).getAddress == mailingList(0))
        assert(toAddresses(1).getAddress == mailingList(1))
      }

    }

    describe("createEmptyMessage()") {

      it("should create message with correct 'from'") {
        val msg = Emailer.createEmptyMessage("", "subject", doug_home, List(doug_home), Nil, Nil)
        val frmAddresses = msg.get.getFrom
        assert(frmAddresses.length == 1)
        assert(frmAddresses(0).asInstanceOf[InternetAddress].getAddress == doug_home)
      }

      it("should create message with correct 'subject'") {
        val randomSubject = (new Random).nextInt() + "bottles of beer"

        val msg = Emailer.createEmptyMessage("", randomSubject, doug_home, List(doug_home), Nil, Nil)
        assert(msg.get.getSubject == randomSubject)

      }

      it("should create message with correct 'to'") {
        val msg = Emailer.createEmptyMessage("", "", doug_home, List(collin_email), Nil, Nil).get
        val toAddresses = msg
          .asInstanceOf[MimeMessage]
          .getRecipients(Message.RecipientType.TO)
          .asInstanceOf[Array[InternetAddress]]
        assert(toAddresses.length == 1)
        assert(toAddresses(0).getAddress == collin_email)
      }

      it("should create message with correct 'cc'") {
        val msg = Emailer.createEmptyMessage("", "", doug_home, List(collin_email), List(doug_work), Nil).get
        val ccAddresses = msg
          .asInstanceOf[MimeMessage]
          .getRecipients(Message.RecipientType.CC)
          .asInstanceOf[Array[InternetAddress]]
        assert(ccAddresses.length == 1)
        assert(ccAddresses(0).getAddress == doug_work)
      }

      it("should create message with correct 'bcc'") {
        val msg = Emailer.createEmptyMessage("", "", doug_home, Nil, Nil, List(doug_home)).get
        val bccAddresses = msg
          .asInstanceOf[MimeMessage]
          .getRecipients(Message.RecipientType.BCC)
          .asInstanceOf[Array[InternetAddress]]
        assert(bccAddresses.length == 1)
        assert(bccAddresses(0).getAddress == doug_home)
      }

    }

    describe("EmailConfig") {

      describe("variable substitution works") {

        it("smtpHost") {
          val dict = new VarSubstitution
          val mailer = "MAILER"
          dict.addString("smtpHost", mailer)
          val sut = EmailConfig("${smtpHost}", "subject", "from", List("to"))
          assert(sut.substituteVariables(dict) == sut.copy(smtpHost = mailer))
        }

        it("subject") {
          val dict = new VarSubstitution
          val SUB = "SUBJECT"
          dict.addString("subject", SUB)
          val sut = EmailConfig("smtpHost", "${subject}", "from", List("to"))
          assert(sut.substituteVariables(dict) == sut.copy(subject = SUB))
        }

        it("from") {
          val dict = new VarSubstitution
          val FROM = "root"
          dict.addString("from", FROM)
          val sut = EmailConfig("smtpHost", "subject", "$from", List("to"))
          assert(sut.substituteVariables(dict) == sut.copy(from = FROM))
        }

        it("to") {
          val dict = new VarSubstitution
          val TO = "Doug"
          dict.addString("to", TO)
          val sut = EmailConfig("smtpHost", "subject", "from", List("$to"))
          assert(sut.substituteVariables(dict) == sut.copy(to = List(TO)))
        }

        it("cc") {
          val dict = new VarSubstitution
          val CC = "Patrick"
          dict.addString("cc", CC)
          val sut = EmailConfig(
            "smtpHost",
            "subject",
            "from",
            List("to"),
            Some(List("$cc"))
          )
          assert(sut.substituteVariables(dict) == sut.copy(cc = Some(List(CC))))
        }

        it("bcc") {
          val dict = new VarSubstitution
          val BCC = "John"
          dict.addString("bcc", BCC)
          val sut = EmailConfig(
            "smtpHost",
            "subject",
            "from",
            List("to"),
            None,
            Some(List("$bcc"))
          )
          assert(sut.substituteVariables(dict) == sut.copy(bcc = Some(List(BCC))))
        }

      }

      describe("sets fields properly from EmailConfig") {

        val HOST = "smtpHost"
        val SUB = "subject"
        val FROM = doug_work
        val TO = doug_home
        val CC = doug_work
        val BCC1 = collin_email
        val BCC2 = patrick_email
        val BCC = BCC1 + "," + BCC2

        val msg = Emailer.createEmptyMessage(
          EmailConfig(
            HOST,
            SUB,
            FROM,
            List(TO),
            Some(List(CC)),
            Some(List(BCC))
          )
        )

        it("creates Message") {
          assert(msg.isDefined)
        }

        it("subject") {
          assert(msg.get.getSubject == SUB)
        }

        it("from") {
          val frmAddresses = msg.get.getFrom
          assert(frmAddresses.length == 1)
          assert(
            frmAddresses(0).asInstanceOf[InternetAddress].getAddress == FROM
          )
        }

        it("to") {
          val toAddresses = msg.get
            .asInstanceOf[MimeMessage]
            .getRecipients(Message.RecipientType.TO)
            .asInstanceOf[Array[InternetAddress]]
          assert(toAddresses.length == 1)
          assert(toAddresses(0).getAddress == TO)
        }

        it("cc") {
          val ccAddresses = msg.get
            .asInstanceOf[MimeMessage]
            .getRecipients(Message.RecipientType.CC)
            .asInstanceOf[Array[InternetAddress]]
          assert(ccAddresses.length == 1)
          assert(ccAddresses(0).getAddress == CC)
        }

        it("bcc") {
          val bccAddresses = msg.get
            .asInstanceOf[MimeMessage]
            .getRecipients(Message.RecipientType.BCC)
            .asInstanceOf[Array[InternetAddress]]
          assert(bccAddresses.length == 2)
          assert(bccAddresses(0).getAddress == BCC1)
          assert(bccAddresses(1).getAddress == BCC2)
        }

      }

    }

  }

}
