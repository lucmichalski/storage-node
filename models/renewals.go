package models

import (
	"encoding/hex"
	"errors"
	"github.com/jinzhu/gorm"
	"github.com/opacity/storage-node/services"
	"github.com/opacity/storage-node/utils"
	"math/big"
	"time"
)

type Renewal struct {
	/*AccountID associates an entry in the renewals table with an entry in the accounts table*/
	AccountID     string            `gorm:"primary_key" json:"accountID" binding:"required,len=64"`
	CreatedAt     time.Time         `json:"createdAt"`
	UpdatedAt     time.Time         `json:"updatedAt"`
	EthAddress    string            `json:"ethAddress" binding:"required,len=42" minLength:"42" maxLength:"42" example:"a 42-char eth address with 0x prefix"` // the eth address they will send payment to
	EthPrivateKey string            `json:"ethPrivateKey" binding:"required,len=96"`                                                                           // the private key of the eth address
	PaymentStatus PaymentStatusType `json:"paymentStatus" binding:"required"`                                                                                  // the status of their payment
	ApiVersion    int               `json:"apiVersion" binding:"omitempty,gte=1" gorm:"default:1"`
	PaymentMethod PaymentMethodType `json:"paymentMethod" gorm:"default:0"`
	OpctCost      float64           `json:"opctCost" binding:"omitempty,gte=0" example:"1.56"`
	//UsdCost          float64           `json:"usdcost" binding:"omitempty,gte=0" example:"39.99"`
	DurationInMonths int `json:"durationInMonths" gorm:"default:12" binding:"required,gte=1" minimum:"1" example:"12"`
}

/*RenewalCollectionFunctions maps a PaymentStatus to the method that should be run
on an renewal of that status*/
var RenewalCollectionFunctions = make(map[PaymentStatusType]func(
	renewal Renewal) error)

func init() {
	RenewalCollectionFunctions[InitialPaymentInProgress] = handleRenewalWithPaymentInProgress
	RenewalCollectionFunctions[InitialPaymentReceived] = handleRenewalThatNeedsGas
	RenewalCollectionFunctions[GasTransferInProgress] = handleRenewalReceivingGas
	RenewalCollectionFunctions[GasTransferComplete] = handleRenewalReadyForCollection
	RenewalCollectionFunctions[PaymentRetrievalInProgress] = handleRenewalWithCollectionInProgress
	RenewalCollectionFunctions[PaymentRetrievalComplete] = handleRenewalAlreadyCollected
}

/*BeforeCreate - callback called before the row is created*/
func (renewal *Renewal) BeforeCreate(scope *gorm.Scope) error {
	if renewal.PaymentStatus < InitialPaymentInProgress {
		renewal.PaymentStatus = InitialPaymentInProgress
	}
	return utils.Validator.Struct(renewal)
}

/*BeforeUpdate - callback called before the row is updated*/
func (renewal *Renewal) BeforeUpdate(scope *gorm.Scope) error {
	return utils.Validator.Struct(renewal)
}

/*BeforeDelete - callback called before the row is deleted*/
func (renewal *Renewal) BeforeDelete(scope *gorm.Scope) error {
	DeleteStripePaymentIfExists(renewal.AccountID)
	return nil
}

/*GetOrCreateRenewal will either get or create an renewal.  If the renewal already existed it will update the OpctCost
but will not update the EthAddress and EthPrivateKey*/
func GetOrCreateRenewal(renewal Renewal) (*Renewal, error) {
	renewalsFromDB, err := GetRenewalsFromAccountID(renewal.AccountID)
	if err != nil {
		return &Renewal{}, err
	}
	if len(renewalsFromDB) == 0 {
		err = DB.Create(&renewal).Error
	} else {
		renewal = renewalsFromDB[0]
	}

	return &renewal, err
}

/*GetRenewalsFromAccountID gets all renewals that have a particular AccountID*/
func GetRenewalsFromAccountID(accountID string) ([]Renewal, error) {
	var renewals []Renewal
	err := DB.Where("account_id = ?",
		accountID).Find(&renewals).Error
	return renewals, err
}

/*SetRenewalsToNextPaymentStatus transitions an renewal to the next payment status*/
func SetRenewalsToNextPaymentStatus(renewals []Renewal) {
	for _, renewal := range renewals {
		if renewal.PaymentStatus == PaymentRetrievalComplete {
			continue
		}
		err := DB.Model(&renewal).Update("payment_status", getNextPaymentStatus(renewal.PaymentStatus)).Error
		utils.LogIfError(err, nil)
	}
}

/*CheckIfPaid returns whether the renewal has been paid for*/
func (renewal *Renewal) CheckIfPaid() (bool, error) {
	if renewal.PaymentStatus >= InitialPaymentReceived {
		return true, nil
	}
	costInWei := renewal.GetTotalCostInWei()
	paid, err := BackendManager.CheckIfPaid(services.StringToAddress(renewal.EthAddress),
		costInWei)
	if paid {
		SetRenewalsToNextPaymentStatus([]Renewal{*(renewal)})
	}
	return paid, err
}

/*GetTotalCostInWei gets the total cost in wei for an renewal*/
func (renewal *Renewal) GetTotalCostInWei() *big.Int {
	return utils.ConvertToWeiUnit(big.NewFloat(renewal.OpctCost))
}

/*GetRenewalsByPaymentStatus gets renewals based on the payment status passed in*/
func GetRenewalsByPaymentStatus(paymentStatus PaymentStatusType) []Renewal {
	var renewals []Renewal
	err := DB.Where("payment_status = ?",
		paymentStatus).Find(&renewals).Error
	utils.LogIfError(err, nil)
	return renewals
}

/*handleRenewalWithPaymentInProgress checks if the user has paid for their renewal, and if so
sets the renewal to the next payment status.

Not calling SetRenewalsToNextPaymentStatus here because CheckIfPaid calls it
*/
func handleRenewalWithPaymentInProgress(renewal Renewal) error {
	_, err := renewal.CheckIfPaid()
	return err
}

/*handleRenewalThatNeedsGas sends some ETH to an renewal that we will later need to collect tokens from and sets the
renewal's payment status to the next status.*/
func handleRenewalThatNeedsGas(renewal Renewal) error {
	paid, _ := renewal.CheckIfPaid()
	var transferErr error
	if paid {
		_, _, _, transferErr = EthWrapper.TransferETH(
			services.MainWalletAddress,
			services.MainWalletPrivateKey,
			services.StringToAddress(renewal.EthAddress),
			services.DefaultGasForPaymentCollection)
		if transferErr == nil {
			SetRenewalsToNextPaymentStatus([]Renewal{renewal})
			return nil
		}
	}
	return transferErr
}

/*handleRenewalReceivingGas checks whether the gas has arrived and transitions the renewal to the next payment
status if so.*/
func handleRenewalReceivingGas(renewal Renewal) error {
	ethBalance := EthWrapper.GetETHBalance(services.StringToAddress(renewal.EthAddress))
	if ethBalance.Cmp(big.NewInt(0)) > 0 {
		SetRenewalsToNextPaymentStatus([]Renewal{renewal})
	}
	return nil
}

/*handleRenewalReadyForCollection will attempt to retrieve the tokens from the renewal's payment address and set the
renewal's payment status to the next status if there are no errors.*/
func handleRenewalReadyForCollection(renewal Renewal) error {
	tokenBalance := EthWrapper.GetTokenBalance(services.StringToAddress(renewal.EthAddress))
	ethBalance := EthWrapper.GetETHBalance(services.StringToAddress(renewal.EthAddress))
	keyInBytes, decryptErr := utils.DecryptWithErrorReturn(
		utils.Env.EncryptionKey,
		renewal.EthPrivateKey,
		renewal.AccountID,
	)
	privateKey, keyErr := services.StringToPrivateKey(hex.EncodeToString(keyInBytes))

	if err := utils.ReturnFirstError([]error{decryptErr, keyErr}); err != nil {
		return err
	} else if tokenBalance.Cmp(big.NewInt(0)) == 0 {
		return errors.New("expected a token balance but found 0")
	} else if ethBalance.Cmp(big.NewInt(0)) == 0 {
		return errors.New("expected an eth balance but found 0")
	} else if tokenBalance.Cmp(big.NewInt(0)) < 0 {
		return errors.New("got negative balance for tokenBalance")
	} else if ethBalance.Cmp(big.NewInt(0)) < 0 {
		return errors.New("got negative balance for ethBalance")
	}

	success, _, _ := EthWrapper.TransferToken(
		services.StringToAddress(renewal.EthAddress),
		privateKey,
		services.MainWalletAddress,
		*tokenBalance,
		services.SlowGasPrice)
	if success {
		SetRenewalsToNextPaymentStatus([]Renewal{renewal})
		return nil
	}
	return errors.New("payment collection failed")
}

/*handleRenewalWithCollectionInProgress will check the token balance of an renewal's payment address.  If the balance
is zero, it means the collection has succeeded and the payment status is set to the next status*/
func handleRenewalWithCollectionInProgress(renewal Renewal) error {
	balance := EthWrapper.GetTokenBalance(services.StringToAddress(renewal.EthAddress))
	if balance.Cmp(big.NewInt(0)) == 0 {
		SetRenewalsToNextPaymentStatus([]Renewal{renewal})
	}
	return nil
}

func handleRenewalAlreadyCollected(renewal Renewal) error {
	return nil
}

/*PurgeOldRenewals deletes renewals past a certain age*/
func PurgeOldRenewals(hoursToRetain int) error {
	err := DB.Where("updated_at < ?",
		time.Now().Add(-1*time.Hour*time.Duration(hoursToRetain))).Delete(&Renewal{}).Error

	return err
}

/*SetRenewalsToLowerPaymentStatusByUpdateTime sets renewals to a lower payment status if the account has a certain payment
status and the updated_at time is older than the cutoff argument*/
func SetRenewalsToLowerPaymentStatusByUpdateTime(paymentStatus PaymentStatusType, updatedAtCutoffTime time.Time) error {
	return DB.Exec("UPDATE renewals set payment_status = ? WHERE payment_status = ? AND updated_at < ?", paymentStatus-1, paymentStatus, updatedAtCutoffTime).Error
}
