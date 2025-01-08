package nats

import (
	"errors"
	"testing"
)

func Test_wrapError_Unwrap(t *testing.T) {
	err := wrapError(ErrInvalidJSAck, "external error message")
	if !errors.Is(err, ErrInvalidJSAck) {
		t.Errorf("wrapError() = %v, want %v", err, ErrInvalidJSAck)
	}

	err = wrapError(err, "")
	if !errors.Is(err, ErrInvalidJSAck) {
		t.Errorf("wrapError() = %v, want %v", err, ErrInvalidJSAck)
	}
}

func Test_wrapError(t *testing.T) {
	type args struct {
		err     error
		message string
	}
	tests := []struct {
		name      string
		args      args
		wantNil   bool
		wantError string
	}{
		{
			name:      "test1",
			args:      args{err: nil},
			wantNil:   true,
			wantError: "",
		},
		{
			name: "test2",
			args: args{
				err:     ErrInvalidJSAck,
				message: "test",
			},
			wantNil:   false,
			wantError: "nats: invalid jetstream publish response: test",
		},
		{
			name: "test2",
			args: args{
				err:     ErrInvalidJSAck,
				message: "",
			},
			wantNil:   false,
			wantError: "nats: invalid jetstream publish response: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapError(tt.args.err, tt.args.message)
			if tt.wantNil {
				if got != nil {
					t.Errorf("wrapError() = %v, want nil", got)
				}
				return
			}

			if got == nil {
				t.Errorf("wrapError() = %v, want not nil", got)
			} else if got.Error() != tt.wantError {
				t.Errorf("wrapError() = %v, want %v", got, tt.wantError)
			}
		})
	}
}
