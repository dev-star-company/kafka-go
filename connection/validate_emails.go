package connection

import "github.com/dev-star-company/custom-validate/validate"

func ValidateEmailCreate(email SyncEmailStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"UserUuid":  "required,uuid",
		"Email":     "required,email",
		"CreatedAt": "required",
		"UpdatedAt": "required",
		"CreatedBy": "required,numeric,min=1",
		"UpdatedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, email); err != nil {
		return err
	}

	return nil
}

func ValidateEmailUpdate(email SyncEmailStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"UserUuid":  "required,uuid",
		"Email":     "omitempty,email",
		"CreatedAt": "omitempty",
		"UpdatedAt": "required",
		"CreatedBy": "omitempty,numeric,min=1",
		"UpdatedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, email); err != nil {
		return err
	}

	return nil
}

func ValidateEmailDelete(email SyncEmailStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"DeletedAt": "required",
		"DeletedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, email); err != nil {
		return err
	}

	return nil
}
