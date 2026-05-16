package model

type LoginReq struct {
	Username string `json:"username" example:"ckman"`
	Password string `json:"password" example:"63cb91a2ceb9d4f7c8b1ba5e50046f52"`
}

type LoginRsp struct {
	Username string `json:"username" example:"ckman"`
	Token    string `json:"token" example:"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"`
	Policy   string `json:"policy" example:"admin"`
	Enabled  bool   `json:"enabled" example:"true"`
}

// CkmanUser is the persisted user record. PasswordHash is never serialized to JSON.
type CkmanUser struct {
	ID           int64  `json:"-"`
	Username     string `json:"username"`
	PasswordHash string `json:"-"`
	Policy       string `json:"policy"`
	Enabled      bool   `json:"enabled"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type UserListItem struct {
	Username  string `json:"username"`
	Policy    string `json:"policy"`
	Enabled   bool   `json:"enabled"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type CreateUserReq struct {
	Username string `json:"username" example:"alice"`
	Password string `json:"password" example:"PlainPass1!"`
	Policy   string `json:"policy" example:"ordinary"`
	Enabled  bool   `json:"enabled" example:"true"`
}

type UpdateUserReq struct {
	Policy  string `json:"policy" example:"guest"`
	Enabled *bool  `json:"enabled"`
}

type ChangeMyPasswordReq struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

type ResetPasswordReq struct {
	NewPassword string `json:"new_password"`
}
