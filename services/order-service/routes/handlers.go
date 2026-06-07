package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	orderv1 "github.com/yourusername/oms/gen/order/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type placeBody struct {
	Symbol       string `json:"symbol"`
	Exchange     string `json:"exchange"`
	Side         string `json:"side"`
	OrderType    string `json:"order_type"`
	Quantity     int64  `json:"quantity"`
	Price        int64  `json:"price"`
	ClientID     string `json:"client_id"`
	TriggerPrice int64  `json:"trigger_price"`
}

type modifyBody struct {
	Quantity     int64 `json:"quantity"`
	Price        int64 `json:"price"`
	TriggerPrice int64 `json:"trigger_price"`
}

type cancelBody struct {
	ClientID string `json:"client_id"`
}

func (rt *Router) place(c *gin.Context) {
	var b placeBody
	if err := c.ShouldBindJSON(&b); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
		return
	}

	resp, err := rt.orders.PlaceOrder(c.Request.Context(), &orderv1.PlaceOrderRequest{
		Symbol:       b.Symbol,
		Exchange:     orderv1.Exchange(orderv1.Exchange_value[b.Exchange]),
		Side:         orderv1.Side(orderv1.Side_value[b.Side]),
		OrderType:    orderv1.OrderType(orderv1.OrderType_value[b.OrderType]),
		Quantity:     b.Quantity,
		Price:        b.Price,
		ClientId:     b.ClientID,
		TriggerPrice: b.TriggerPrice,
	})
	if err != nil {
		writeError(c, err)
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"order_id": resp.OrderId,
		"status":   resp.Status.String(),
		"message":  resp.Message,
	})
}

func (rt *Router) status(c *gin.Context) {
	resp, err := rt.orders.GetOrderStatus(c.Request.Context(), &orderv1.OrderStatusRequest{
		OrderId: c.Param("id"),
	})
	if err != nil {
		writeError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"order_id":        resp.OrderId,
		"symbol":          resp.Symbol,
		"exchange":        resp.Exchange.String(),
		"side":            resp.Side.String(),
		"order_type":      resp.OrderType.String(),
		"quantity":        resp.Quantity,
		"filled_quantity": resp.FilledQuantity,
		"price":           resp.Price,
		"status":          resp.Status.String(),
	})
}

func (rt *Router) modify(c *gin.Context) {
	var b modifyBody
	if err := c.ShouldBindJSON(&b); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
		return
	}

	resp, err := rt.orders.ModifyOrder(c.Request.Context(), &orderv1.ModifyOrderRequest{
		OrderId:      c.Param("id"),
		Quantity:     b.Quantity,
		Price:        b.Price,
		TriggerPrice: b.TriggerPrice,
	})
	if err != nil {
		writeError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"order_id": resp.OrderId,
		"status":   resp.Status.String(),
		"message":  resp.Message,
	})
}

func (rt *Router) cancel(c *gin.Context) {
	var b cancelBody
	_ = c.ShouldBindJSON(&b) // body is optional on DELETE

	resp, err := rt.orders.CancelOrder(c.Request.Context(), &orderv1.CancelOrderRequest{
		OrderId:  c.Param("id"),
		ClientId: b.ClientID,
	})
	if err != nil {
		writeError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"order_id": resp.OrderId,
		"status":   resp.Status.String(),
		"message":  resp.Message,
	})
}

func (rt *Router) metricsSnapshot(c *gin.Context) {
	c.JSON(http.StatusOK, rt.metrics.Snapshot())
}

func writeError(c *gin.Context, err error) {
	st := status.Convert(err)
	c.JSON(httpStatus(st.Code()), gin.H{
		"error": st.Message(),
		"code":  st.Code().String(),
	})
}

func httpStatus(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
