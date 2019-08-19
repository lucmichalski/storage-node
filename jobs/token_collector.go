package jobs

import (
	"github.com/opacity/storage-node/models"
	"github.com/opacity/storage-node/services"
	"github.com/opacity/storage-node/utils"
	"time"
)

type tokenCollector struct{}

func (t tokenCollector) Name() string {
	return "tokenCollector"
}

func (t tokenCollector) ScheduleInterval() string {
	return "@every 5m"
}

func (t tokenCollector) Run() {
	utils.SlackLog("running " + t.Name())
	for paymentStatus := models.InitialPaymentInProgress; paymentStatus < models.PaymentRetrievalComplete; paymentStatus++ {
		utils.SlackLog("about to get accounts with payment status " + models.PaymentStatusMap[paymentStatus])
		utils.SlackLog(time.Now().String())
		accounts := models.GetAccountsByPaymentStatus(paymentStatus)
		utils.SlackLog("got accounts with payment status " + models.PaymentStatusMap[paymentStatus])
		utils.SlackLog(time.Now().String())
		runCollectionSequence(accounts)
	}
}

func (t tokenCollector) Runnable() bool {
	err := services.SetWallet()
	utils.LogIfError(err, nil)
	return models.DB != nil && err == nil
}

func runCollectionSequence(accounts []models.Account) {
	for _, account := range accounts {
		utils.SlackLog("about to run PaymentCollectionFunction")
		utils.SlackLog(time.Now().String())
		err := models.PaymentCollectionFunctions[account.PaymentStatus](account)
		cost, _ := account.Cost()
		utils.SlackLog("ran PaymentCollectionFunction")
		utils.SlackLog(time.Now().String())
		utils.LogIfError(err, map[string]interface{}{
			"eth_address":    account.EthAddress,
			"account_id":     account.AccountID,
			"payment_status": models.PaymentStatusMap[account.PaymentStatus],
			"cost":           cost,
		})
	}
}
