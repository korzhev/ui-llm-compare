package model

type LLMTextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type LLMImgURLContent struct {
	URL string `json:"url"`
}

type LLMImgContent struct {
	Type      string           `json:"type"`
	ImgUrl    LLMImgURLContent `json:"image_url"`
	MinPixels int              `json:"min_pixels"`
	MaxPixels int              `json:"max_pixels"`
}

type LLMMsg struct {
	Role    string `json:"role"`
	Content []any  `json:"content"`
}

type LLMRequest struct {
	Model    string   `json:"model"`
	Messages []LLMMsg `json:"messages"`
}

type LLMResponseChoice struct {
	Message LLMResponseMessage `json:"message"`
}

type LLMResponseMessage struct {
	Content string `json:"content"`
}

type LLMResponse struct {
	Choices []LLMResponseChoice `json:"choices"`
}
