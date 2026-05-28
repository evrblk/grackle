package ids

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestNamespaceIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
		}

		actual, err := DecodeNamespaceId(EncodeNamespaceId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestNamespaceIdDecode(t *testing.T) {
	_, err := DecodeNamespaceId("err_lSpiLEvW6NZcMDwH")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_lSpiLEvW6NZcMD")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_lSpiLEvW6NZcMDwHs")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_lSpiLEvW6N+cMDwH")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_lSpiLEvW6NZcMDwH")
	require.NoError(t, err)
}

func TestWaitGroupIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			WaitGroupId: rand.Uint64(),
		}

		actual, err := DecodeWaitGroupId(EncodeWaitGroupId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestWaitGroupIdDecode(t *testing.T) {
	_, err := DecodeWaitGroupId("err_foppxIQOniRDPNpqH8xKzNLV3YJ")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_foppxIQOniRDPNpqH8xKzNLV3")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_foppxIQOniRDPNpqH8xKzNLV3YJds")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_foppxIQOniRDPNpqH8+KzNLV3YJ")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_foppxIQOniRDPNpqH8xKzNLV3YJ")
	require.NoError(t, err)
}

func TestSemaphoreIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			SemaphoreId: rand.Uint64(),
		}

		actual, err := DecodeSemaphoreId(EncodeSemaphoreId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestSemaphoreIdDecode(t *testing.T) {
	_, err := DecodeSemaphoreId("err_NfKKeiPbP18NFeU3lLGrRWWgDJRB")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_NfKKeiPbP18NFeU3lLGrRWWgDJRBsd")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_NfKKeiPbP18NFeU3lLGrRWWgDB")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_NfKKeiPbP18NFeU3lLGr+WWgDJRB")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_NfKKeiPbP18NFeU3lLGrRWWgDJRB")
	require.NoError(t, err)
}

func TestBarrierIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			BarrierId:   rand.Uint64(),
		}

		actual, err := DecodeBarrierId(EncodeBarrierId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestBarrierIdDecode(t *testing.T) {
	_, err := DecodeBarrierId("err_NfKKeiPbP18NFeU3lLGrRWWgDJRB")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_NfKKeiPbP18NFeU3lLGrRWWgDJRBsd")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_NfKKeiPbP18NFeU3lLGrRWWgDB")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_NfKKeiPbP18NFeU3lLGr+WWgDJRB")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_NfKKeiPbP18NFeU3lLGrRWWgDJRB")
	require.NoError(t, err)
}

func TestLeaseIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.LeaseId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint32(),
			LeaseId:     rand.Uint64(),
		}

		actual, err := DecodeLeaseId(EncodeLeaseId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestLeaseIdDecode(t *testing.T) {
	_, err := DecodeLeaseId("err_NfKKeiPbP18NFeU3lLGrRWWgDJRB")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_NfKKeiPbP18NFeU3lLGrRWWgDJRBsd")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_NfKKeiPbP18NFeU3lLGrRWWgDB")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_NfKKeiPbP18NFeU3lLGr+WWgDJRB")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB")
	require.NoError(t, err)
}
