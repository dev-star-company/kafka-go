package connection

import "github.com/dev-star-company/custom-validate/validate"

func ValidatePasswordCreate(password SyncPasswordStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"UserUuid":  "required,uuid",
		"Password":  "required,min=8",
		"CreatedAt": "required",
		"UpdatedAt": "required",
		"CreatedBy": "required,numeric,min=1",
		"UpdatedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, password); err != nil {
		return err
	}

	return nil
}

func ValidatePasswordUpdate(password SyncPasswordStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"UserUuid":  "required,uuid",
		"Password":  "omitempty,min=8",
		"CreatedAt": "omitempty",
		"UpdatedAt": "required",
		"CreatedBy": "omitempty,numeric,min=1",
		"UpdatedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, password); err != nil {
		return err
	}

	return nil
}

func ValidatePasswordDelete(password SyncPasswordStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"DeletedAt": "required",
		"DeletedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, password); err != nil {
		return err
	}

	return nil
}
