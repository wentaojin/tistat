/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"fmt"

	"github.com/wentaojin/tistat/config"
	"gopkg.in/gomail.v2"
)

type Email struct {
	Receivers []string
	Subject   string
	Msg       string
}

func NewEmail(receivers []string, subject, msg string) *Email {
	return &Email{
		Receivers: receivers,
		Subject:   subject,
		Msg:       msg,
	}
}

func (e *Email) SendEmail(sendCfg config.AlertConfig) error {
	email := gomail.NewMessage()
	email.SetHeader("From", sendCfg.SendEmail)
	email.SetHeader("To", sendCfg.ReceiveEmails...)
	//email.SetAddressHeader("Cc","test@126.com","test") // 抄送人
	email.SetHeader("Subject", e.Subject)
	email.SetBody("text/html", e.Msg)
	//email.Attach("/data/marvin.png") // 邮件附件

	// 使用邮件邮箱服务器授权码
	send := gomail.NewDialer(
		sendCfg.SmtpHost,
		sendCfg.SmtpPort,
		sendCfg.SendEmail,
		sendCfg.SendEmailAuthPWD,
	)
	if err := send.DialAndSend(email); err != nil {
		return fmt.Errorf("error on send email failed: %v", err)
	}

	return nil
}

func RenderAlertTemplateMSG(rows []WaitStatsTable) string {
	// 邮件表格样式
	var emailTemplate = `<style>
table {
	border-collapse: collapse;
}
th {
	background-color: #007fff;
	color: white;
}
table, th, td {
	border: 1px solid black;
	padding: 5px;
	text-align: left;
}
</style>
<table>
	<tr>
		<th>ServerAddr</th>
		<th>DBVersion</th>
		<th>AnalyzeSQL</th>
		<th>AnalyzeTimeout</th>
		<th>RowCounts</th>
		<th>StatsOutdir</th>
	</tr>`
	for _, r := range rows {
		emailTemplate = emailTemplate + fmt.Sprintf(
			`<tr>
						<td>%v</td>
						<td>%v</td>
						<td>%v</td>
						<td>%d</td>
						<td>%f</td>
						<td>%v</td>
					</tr>`,
			r.ServerAddr,
			r.DBVersion,
			r.AnalyzeSQL,
			r.AnalyzeTimeout,
			r.RowCounts,
			r.StatsOutdir)
	}
	emailTemplate = emailTemplate + `</table><br>Send By tistat@marvin.com</br> (请人工关注处理，自动发送请勿回复)`

	return emailTemplate
}
