package routes

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/opacity/storage-node/models"
	"github.com/opacity/storage-node/services"
	"github.com/opacity/storage-node/utils"
	"math/big"
	"time"
)

type getUpgradeAccountInvoiceObject struct {
	StorageLimit     int `json:"storageLimit" binding:"required,gte=128" minimum:"128" example:"128"`
	DurationInMonths int `json:"durationInMonths" binding:"required,gte=1" minimum:"1" example:"12"`
}

type checkUpgradeStatusObject struct {
	MetadataKeys     []string `json:"metadataKeys" binding:"required" example:"an array containing all your metadata keys"`
	FileHandles      []string `json:"fileHandles" binding:"required" example:"an array containing all your file handles"`
	StorageLimit     int      `json:"storageLimit" binding:"required,gte=128" minimum:"128" example:"128"`
	DurationInMonths int      `json:"durationInMonths" binding:"required,gte=1" minimum:"1" example:"12"`
}

type getUpgradeAccountInvoiceReq struct {
	verification
	requestBody
	getUpgradeAccountInvoiceObject getUpgradeAccountInvoiceObject
}

type checkUpgradeStatusReq struct {
	verification
	requestBody
	checkUpgradeStatusObject checkUpgradeStatusObject
}

type getUpgradeAccountInvoiceRes struct {
	OpqInvoice models.Invoice `json:"opqInvoice"`
	UsdInvoice float64        `json:"usdInvoice"`
}

func (v *getUpgradeAccountInvoiceReq) getObjectRef() interface{} {
	return &v.getUpgradeAccountInvoiceObject
}

func (v *checkUpgradeStatusReq) getObjectRef() interface{} {
	return &v.checkUpgradeStatusObject
}

// GetAccountUpgradeInvoiceHandler godoc
// @Summary get an invoice to upgrade an account
// @Description get an invoice to upgrade an account
// @Accept  json
// @Produce  json
// @Param getUpgradeAccountInvoiceReq body routes.getUpgradeAccountInvoiceReq true "get upgrade invoice object"
// @description requestBody should be a stringified version of (values are just examples):
// @description {
// @description 	"storageLimit": 100,
// @description 	"durationInMonths": 12,
// @description }
// @Success 200 {object} routes.getUpgradeAccountInvoiceRes
// @Failure 400 {string} string "bad request, unable to parse request body: (with the error)"
// @Failure 404 {string} string "no account with that id: (with your accountID)"
// @Failure 500 {string} string "some information about the internal error"
// @Router /api/v1/upgrade/invoice [post]
/*GetAccountUpgradeInvoiceHandler is a handler for getting an invoice to upgrade an account*/
func GetAccountUpgradeInvoiceHandler() gin.HandlerFunc {
	return ginHandlerFunc(getAccountUpgradeInvoice)
}

// CheckUpgradeStatusHandler godoc
// @Summary check the upgrade status
// @Description check the upgrade status
// @Accept  json
// @Produce  json
// @Param checkUpgradeStatusReq body routes.checkUpgradeStatusReq true "check upgrade status object"
// @description requestBody should be a stringified version of (values are just examples):
// @description {
// @description 	"storageLimit": 100,
// @description 	"durationInMonths": 12,
// @description 	"metadataKeys": "["someKey", "someOtherKey]",
// @description 	"fileHandles": "["someHandle", "someOtherHandle]",
// @description }
// @Success 200 {object} routes.StatusRes
// @Failure 400 {string} string "bad request, unable to parse request body: (with the error)"
// @Failure 404 {string} string "no account with that id: (with your accountID)"
// @Failure 500 {string} string "some information about the internal error"
// @Router /api/v1/upgrade [post]
/*CheckUpgradeStatusHandler is a handler for checking the upgrade status*/
func CheckUpgradeStatusHandler() gin.HandlerFunc {
	return ginHandlerFunc(checkUpgradeStatus)
}

func getAccountUpgradeInvoice(c *gin.Context) error {
	request := getUpgradeAccountInvoiceReq{}

	if err := verifyAndParseBodyRequest(&request, c); err != nil {
		return err
	}

	account, err := request.getAccount(c)
	if err != nil {
		return err
	}

	if err := verifyUpgradeEligible(account, request.getUpgradeAccountInvoiceObject.StorageLimit, c); err != nil {
		return err
	}

	upgradeCostInOPQ, _ := account.UpgradeCostInOPQ(request.getUpgradeAccountInvoiceObject.StorageLimit,
		request.getUpgradeAccountInvoiceObject.DurationInMonths)
	upgradeCostInUSD, _ := account.UpgradeCostInUSD(request.getUpgradeAccountInvoiceObject.StorageLimit,
		request.getUpgradeAccountInvoiceObject.DurationInMonths)

	ethAddr, privKey, err := services.EthWrapper.GenerateWallet()
	if err != nil {
		err = fmt.Errorf("error generating upgrade wallet:  %v", err)
		return BadRequestResponse(c, err)
	}

	encryptedKeyInBytes, encryptErr := utils.EncryptWithErrorReturn(
		utils.Env.EncryptionKey,
		privKey,
		account.AccountID,
	)

	if encryptErr != nil {
		return ServiceUnavailableResponse(c, fmt.Errorf("error encrypting private key:  %v", encryptErr))
	}

	upgrade := models.Upgrade{
		AccountID:        account.AccountID,
		NewStorageLimit:  models.StorageLimitType(request.getUpgradeAccountInvoiceObject.StorageLimit),
		OldStorageLimit:  account.StorageLimit,
		EthAddress:       ethAddr.String(),
		EthPrivateKey:    hex.EncodeToString(encryptedKeyInBytes),
		PaymentStatus:    models.InitialPaymentInProgress,
		OpqCost:          upgradeCostInOPQ,
		UsdCost:          upgradeCostInUSD,
		DurationInMonths: request.getUpgradeAccountInvoiceObject.DurationInMonths,
	}

	upgradeInDB, err := models.GetOrCreateUpgrade(upgrade)
	if err != nil {
		err = fmt.Errorf("error getting or creating upgrade:  %v", err)
		return ServiceUnavailableResponse(c, err)
	}

	return OkResponse(c, getUpgradeAccountInvoiceRes{
		OpqInvoice: models.Invoice{
			Cost:       upgradeCostInOPQ,
			EthAddress: upgradeInDB.EthAddress,
		},
		UsdInvoice: upgradeCostInUSD,
	})
}

func checkUpgradeStatus(c *gin.Context) error {
	request := checkUpgradeStatusReq{}

	if err := verifyAndParseBodyRequest(&request, c); err != nil {
		return err
	}

	account, err := request.getAccount(c)
	if err != nil {
		return err
	}

	if err := verifyUpgradeEligible(account, request.checkUpgradeStatusObject.StorageLimit, c); err != nil {
		return err
	}

	upgrade, err := models.GetUpgradeFromAccountIDAndStorageLimits(account.AccountID, request.checkUpgradeStatusObject.StorageLimit, int(account.StorageLimit))
	if upgrade.DurationInMonths != request.checkUpgradeStatusObject.DurationInMonths {
		return ForbiddenResponse(c, errors.New("durationInMonths does not match durationInMonths "+
			"when upgrade was initiated"))
	}

	//stripePayment, err := models.GetNewestStripePaymentByAccountId(account.AccountID)
	//if stripePayment.AccountID == account.AccountID && err == nil && stripePayment.UpgradePayment {
	//	paid, err := stripePayment.CheckChargePaid()
	//	if err != nil {
	//		return InternalErrorResponse(c, err)
	//	}
	//	if !paid {
	//		return OkResponse(c, StatusRes{
	//			Status: "Incomplete",
	//		})
	//	}
	//	stripePayment.CheckUpgradeOPQTransaction(account, request.checkUpgradeStatusObject.StorageLimit)
	//	amount, err := checkChargeAmount(c, stripePayment.ChargeID)
	//	if err != nil {
	//		return InternalErrorResponse(c, err)
	//	}
	//	if amount >= upgrade.UsdCost {
	//		if err := upgradeAccountAndUpdateExpireDates(account, request, c); err != nil {
	//			return InternalErrorResponse(c, err)
	//		}
	//		return OkResponse(c, StatusRes{
	//			Status: "Success with Stripe",
	//		})
	//	}
	//}

	paid, err := models.BackendManager.CheckIfPaid(services.StringToAddress(upgrade.EthAddress),
		utils.ConvertToWeiUnit(big.NewFloat(upgrade.OpqCost)))
	if err != nil {
		return InternalErrorResponse(c, err)
	}
	if !paid {
		return OkResponse(c, StatusRes{
			Status: "Incomplete",
		})
	}
	if err := models.DB.Model(&upgrade).Update("payment_status", models.InitialPaymentReceived).Error; err != nil {
		return InternalErrorResponse(c, err)
	}
	if err := upgradeAccountAndUpdateExpireDates(account, request, c); err != nil {
		return InternalErrorResponse(c, err)
	}
	return OkResponse(c, StatusRes{
		Status: "Success with OPQ",
	})
}

func upgradeAccountAndUpdateExpireDates(account models.Account, request checkUpgradeStatusReq, c *gin.Context) error {
	if err := account.UpgradeAccount(request.checkUpgradeStatusObject.StorageLimit,
		request.checkUpgradeStatusObject.DurationInMonths); err != nil {
		return err
	}
	filesErr := models.UpdateExpiredAt(request.checkUpgradeStatusObject.FileHandles,
		request.verification.PublicKey, account.ExpirationDate())
	metadatasErr := updateMetadataExpiration(request.checkUpgradeStatusObject.MetadataKeys,
		request.verification.PublicKey, account.ExpirationDate(), c)

	return utils.CollectErrors([]error{filesErr, metadatasErr})
}

func updateMetadataExpiration(metadataKeys []string, key string, newExpiredAtTime time.Time, c *gin.Context) error {
	var kvPairs = make(utils.KVPairs)
	var kvKeys utils.KVKeys

	for _, metadataKey := range metadataKeys {
		permissionHashKey := getPermissionHashKeyForBadger(metadataKey)
		permissionHashValue, _, err := utils.GetValueFromDynamoKv(permissionHashKey)
		if err != nil {
			return err
		}

		if err := verifyPermissions(key, metadataKey,
			permissionHashValue, c); err != nil {
			return err
		}
		kvPairs[permissionHashKey] = permissionHashValue
		kvKeys = append(kvKeys, metadataKey)
	}

	kvs, err := utils.BatchGetFromDynamoKv(&kvKeys)
	if err != nil {
		return err
	}
	for key, value := range *kvs {
		kvPairs[key] = value
	}

	if err := utils.BatchSetToDynamoKv(&kvPairs, time.Until(newExpiredAtTime)); err != nil {
		return err
	}

	return nil
}
