package connection

import "github.com/dev-star-company/custom-validate/validate"

func ValidateUserCreate(user SyncUserStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"Name":      "required,min=3",
		"Surname":   "required,min=3",
		"CreatedAt": "required",
		"UpdatedAt": "required",
		"CreatedBy": "required,numeric,min=1",
		"UpdatedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, user); err != nil {
		return err
	}

	for _, phone := range user.Phones {
		if err := ValidatePhoneCreate(phone); err != nil {
			return err
		}
	}

	for _, email := range user.Emails {
		if err := ValidateEmailCreate(email); err != nil {
			return err
		}
	}

	return nil
}

func ValidateUserUpdate(user SyncUserStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"Name":      "omitempty,min=3",
		"Surname":   "omitempty,min=3",
		"UpdatedAt": "required",
		"UpdatedBy": "required,numeric,min=1",
		"DeletedAt": "omitempty",
		"DeletedBy": "omitempty,numeric,min=1",
	}

	if err := validate.Validate(fields, user); err != nil {
		return err
	}

	return nil
}

func ValidateUserDelete(user SyncUserStruct) error {
	fields := map[string]string{
		"Uuid":      "required,uuid",
		"DeletedAt": "required",
		"DeletedBy": "required,numeric,min=1",
	}

	if err := validate.Validate(fields, user); err != nil {
		return err
	}

	return nil
}
