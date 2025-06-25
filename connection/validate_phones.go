package connection

import "github.com/dev-star-company/custom-validate/validate"

func ValidatePhoneCreate(phone SyncPhoneStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"UserUuid":  "required,uuid",
		"Main":      "omitempty,boolean",
		"Phone":     "required,min=3",
		"CreatedAt": "required",
		"UpdatedAt": "required",
		"CreatedBy": "required,numeric,min=1",
		"UpdatedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, phone); err != nil {
		return err
	}

	return nil
}

func ValidatePhoneUpdate(phone SyncPhoneStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"UserUuid":  "required,uuid",
		"Main":      "omitempty,boolean",
		"Phone":     "omitempty,min=3",
		"CreatedAt": "omitempty",
		"UpdatedAt": "required",
		"CreatedBy": "omitempty,numeric,min=1",
		"UpdatedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, phone); err != nil {
		return err
	}

	return nil
}

func ValidatePhoneDelete(phone SyncPhoneStruct) error {
	fields := map[string]string{
		"Uuid":       "required,uuid",
		"DetectedAt": "required",
		"DetectedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, phone); err != nil {
		return err
	}

	return nil
}
