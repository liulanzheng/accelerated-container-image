/*
   Copyright The Accelerated Container Image Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity"
	"github.com/moby/sys/mountinfo"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const (
	// tgt related
	tgtBackingStoreOverlayBD = "overlaybd"

	maxAttachAttempts      = 50
	defaultRollbackTimeout = 30 * time.Second

	obdHbaNum         = 999999999
	obdLoopNaaPreffix = 199
)

// OverlayBDBSConfig is the config of OverlayBD backing store in open-iscsi target.
type OverlayBDBSConfig struct {
	RepoBlobURL string                   `json:"repoBlobUrl"`
	Lowers      []OverlayBDBSConfigLower `json:"lowers"`
	Upper       OverlayBDBSConfigUpper   `json:"upper"`
	ResultFile  string                   `json:"resultFile"`
}

// OverlayBDBSConfigLower
type OverlayBDBSConfigLower struct {
	File   string `json:"file,omitempty"`
	Digest string `json:"digest,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Dir    string `json:"dir,omitempty"`
}

type OverlayBDBSConfigUpper struct {
	Index string `json:"index,omitempty"`
	Data  string `json:"data,omitempty"`
}

// unmountAndDetachBlockDevice
func (o *snapshotter) unmountAndDetachBlockDevice(ctx context.Context, snID string, snKey string) error {
	mountPoint := o.overlaybdMountpoint(snID)
	if err := mount.UnmountAll(mountPoint, 0); err != nil {
		return errors.Wrapf(err, "failed to umount %s", mountPoint)
	}

	loopName := o.overlaybdLoopbackDeviceID(snID)
	loopDir := fmt.Sprintf("/sys/kernel/config/target/loopback/%s", loopName)

	err := os.RemoveAll(path.Join(loopDir, "tpgt_1", "lun", "lun_0", "dev_"+snID))
	if err != nil {
		return errors.Wrapf(err, "failed to remove loopback link %s", path.Join(loopDir, "tpgt_1", "lun", "lun_0", "dev_"+snID))
	}

	err = os.RemoveAll(path.Join(loopDir, "tpgt_1", "lun", "lun_0"))
	if err != nil {
		return errors.Wrapf(err, "failed to remove loopback lun %s", path.Join(loopDir, "tpgt_1", "lun", "lun_0"))
	}

	err = os.RemoveAll(path.Join(loopDir, "tpgt_1"))
	if err != nil {
		return errors.Wrapf(err, "failed to remove loopback tgpt %s", path.Join(loopDir, "tpgt_1"))
	}

	err = os.RemoveAll(loopDir)
	if err != nil {
		return errors.Wrapf(err, "failed to remove loopback dir %s", loopDir)
	}

	devDir := fmt.Sprintf("/sys/kernel/config/target/core/user_%d/dev_%s", obdHbaNum, snID)
	err = os.RemoveAll(devDir)
	if err != nil {
		return errors.Wrapf(err, "failed to remove target dir %s", devDir)
	}
	return nil
}

// attachAndMountBlockDevice
//
// TODO(fuweid): need to track the middle state if the process has been killed.
func (o *snapshotter) attachAndMountBlockDevice(ctx context.Context, snID string, snKey string, writable bool, fsparam string) (retErr error) {
	if err := lookup(o.overlaybdMountpoint(snID)); err == nil {
		return nil
	}

	confPath := o.overlaybdConfPath(snID)
	devDir := fmt.Sprintf("/sys/kernel/config/target/core/user_%d/dev_%s", obdHbaNum, snID)
	err := os.MkdirAll(devDir, 0700)
	if err != nil {
		return errors.Wrapf(err, "failed to create target dir for %s", devDir)
	}

	defer func() {
		if retErr != nil {
			rerr := os.RemoveAll(devDir)
			if rerr != nil {
				log.G(ctx).WithError(rerr).Warnf("failed to clean target dir %s", devDir)
			}
		}
	}()

	err = ioutil.WriteFile(path.Join(devDir, "control"), ([]byte)(fmt.Sprintf("dev_config=overlaybd//%s", confPath)), 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to write target config for %s", devDir)
	}

	err = ioutil.WriteFile(path.Join(devDir, "enable"), ([]byte)("1"), 0666)
	if err != nil {
		// read the init-debug.log for readable
		debugLogPath := o.overlaybdInitDebuglogPath(snID)
		if data, derr := ioutil.ReadFile(debugLogPath); derr == nil {
			return errors.Errorf("failed to enable target for %s, %s", devDir, data)
		}
		return errors.Wrapf(err, "failed to enable target for %s", devDir)
	}

	loopName := o.overlaybdLoopbackDeviceID(snID)
	loopDir := fmt.Sprintf("/sys/kernel/config/target/loopback/%s", loopName)

	err = os.MkdirAll(loopDir, 0700)
	if err != nil {
		return errors.Wrapf(err, "failed to create loopback dir %s", loopDir)
	}

	err = os.MkdirAll(path.Join(loopDir, "tpgt_1", "lun", "lun_0"), 0700)
	if err != nil {
		return errors.Wrapf(err, "failed to create loopback lun dir %s", path.Join(loopDir, "tpgt_1", "lun", "lun_0"))
	}

	defer func() {
		if retErr != nil {
			rerr := os.RemoveAll(path.Join(loopDir, "tpgt_1", "lun", "lun_0"))
			if err != nil {
				log.G(ctx).WithError(rerr).Warnf("failed to clean loopback lun %s", path.Join(loopDir, "tpgt_1", "lun", "lun_0"))
			}

			rerr = os.RemoveAll(path.Join(loopDir, "tpgt_1"))
			if err != nil {
				log.G(ctx).WithError(rerr).Warnf("failed to clean loopback tpgt %s", path.Join(loopDir, "tpgt_1"))
			}

			err = os.RemoveAll(loopDir)
			if err != nil {
				log.G(ctx).WithError(rerr).Warnf("failed to clean loopback dir %s", loopDir)
			}
		}
	}()

	err = ioutil.WriteFile(path.Join(loopDir, "tpgt_1", "nexus"), ([]byte)(loopName), 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to write loopback nexus %s", path.Join(loopDir, "tpgt_1", "nexus"))
	}

	err = os.Symlink(devDir, path.Join(loopDir, "tpgt_1", "lun", "lun_0", "dev_"+snID))
	if err != nil {
		return errors.Wrapf(err, "failed to create loopback link %s", path.Join(loopDir, "tpgt_1", "lun", "lun_0", "dev_"+snID))
	}

	defer func() {
		if retErr != nil {
			rerr := os.RemoveAll(path.Join(loopDir, "tpgt_1", "lun", "lun_0", "dev_"+snID))
			if err != nil {
				log.G(ctx).WithError(rerr).Warnf("failed to clean loopback link %s", path.Join(loopDir, "tpgt_1", "lun", "lun_0", "dev_"+snID))
			}
		}
	}()

	bytes, err := ioutil.ReadFile(path.Join(loopDir, "tpgt_1", "address"))
	if err != nil {
		return errors.Wrapf(err, "failed to read loopback address %s", path.Join(loopDir, "tpgt_1", "address"))
	}
	deviceNumber := string(bytes)
	deviceNumber = strings.TrimSuffix(deviceNumber, "\n")

	params := strings.Split("fsparam", "|")
	fstype := ""
	fsopts := ""
	if len(params) > 0 {
		fstype = params[0]
	}
	if len(params) > 1 {
		fsopts = params[1]
	}

	// The device doesn't show up instantly. Need retry here.
	var lastErr error = nil
	for retry := 0; retry < maxAttachAttempts; retry++ {
		devDirs, err := ioutil.ReadDir("/sys/class/scsi_device/" + deviceNumber + ":0/device/block")
		if err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if len(devDirs) == 0 {
			lastErr = errors.Errorf("empty device found")
			time.Sleep(10 * time.Millisecond)
			continue
		}

		for _, dev := range devDirs {
			device := fmt.Sprintf("/dev/%s", dev.Name())
			var mountPoint = o.overlaybdMountpoint(snID)

			if fstype == "" {
				// fs not set, check fstype on the block
				fstype, err = checkFstypeFromDevice(ctx, device)
				if err != nil {
					lastErr = errors.Wrapf(err, "failed to check fstype for %s", device)
					time.Sleep(10 * time.Millisecond)
					break
				}
			}

			var mflag uintptr = unix.MS_RDONLY
			if writable {
				mflag = 0
			}

			if err := unix.Mount(device, mountPoint, fstype, mflag, fsopts); err != nil {
				lastErr = errors.Wrapf(err, "failed to mount %s to %s", device, mountPoint)
				time.Sleep(10 * time.Millisecond)
				break
			}
			return nil
		}

	}
	return lastErr
}

// constructOverlayBDSpec generates the config spec for OverlayBD backing store.
func (o *snapshotter) constructOverlayBDSpec(ctx context.Context, key string, writable bool) error {
	id, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "failed to get info for snapshot %s", key)
	}

	stype, err := o.identifySnapshotStorageType(id, info)
	if err != nil {
		return errors.Wrapf(err, "failed to identify storage of snapshot %s", key)
	}

	configJSON := OverlayBDBSConfig{
		Lowers:     []OverlayBDBSConfigLower{},
		ResultFile: o.overlaybdInitDebuglogPath(id),
	}

	// load the parent's config and reuse the lowerdir
	if info.Parent != "" {
		parentConfJSON, err := o.loadBackingStoreConfig(ctx, info.Parent)
		if err != nil {
			return err
		}
		configJSON.RepoBlobURL = parentConfJSON.RepoBlobURL
		configJSON.Lowers = parentConfJSON.Lowers
	}

	switch stype {
	case storageTypeRemoteBlock:
		if writable {
			return errors.Errorf("remote block device is readonly, not support writable")
		}

		blobSize, err := strconv.Atoi(info.Labels[labelKeyOverlayBDBlobSize])
		if err != nil {
			return errors.Wrapf(err, "failed to parse value of label %s of snapshot %s", labelKeyOverlayBDBlobSize, key)
		}

		blobDigest := info.Labels[labelKeyOverlayBDBlobDigest]
		blobPrefixURL, err := o.constructImageBlobURL(info.Labels[labelKeyImageRef])
		if err != nil {
			return errors.Wrapf(err, "failed to construct image blob prefix url for snapshot %s", key)
		}

		configJSON.RepoBlobURL = blobPrefixURL
		configJSON.Lowers = append(configJSON.Lowers, OverlayBDBSConfigLower{
			Digest: blobDigest,
			Size:   int64(blobSize),
			Dir:    o.upperPath(id),
		})

	case storageTypeLocalBlock:
		if writable {
			return errors.Errorf("local block device is readonly, not support writable")
		}

		configJSON.Lowers = append(configJSON.Lowers, OverlayBDBSConfigLower{
			Dir: o.upperPath(id),
		})

	default:
		if !writable || info.Parent == "" {
			return errors.Errorf("unexpect storage %v of snapshot %v during construct overlaybd spec(writable=%v, parent=%s)", stype, key, writable, info.Parent)
		}

		if err := o.prepareWritableOverlaybd(ctx, id, info.Parent); err != nil {
			return err
		}

		configJSON.Upper = OverlayBDBSConfigUpper{
			Index: o.overlaybdWritableIndexPath(id),
			Data:  o.overlaybdWritableDataPath(id),
		}
	}
	return o.atomicWriteBackingStoreAndTargetConfig(ctx, id, key, configJSON)
}

// loadBackingStoreConfig loads OverlayBD backing store config.
func (o *snapshotter) loadBackingStoreConfig(ctx context.Context, snKey string) (*OverlayBDBSConfig, error) {
	id, _, _, err := storage.GetInfo(ctx, snKey)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get info of snapshot %s", snKey)
	}

	confPath := o.overlaybdConfPath(id)
	data, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read config(path=%s) of snapshot %s", confPath, snKey)
	}

	var configJSON OverlayBDBSConfig
	if err := json.Unmarshal(data, &configJSON); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal data(%s)", string(data))
	}

	return &configJSON, nil
}

// constructImageBlobURL returns the https://host/v2/<name>/blobs/.
//
// TODO(fuweid): How to know the existing url schema?
func (o *snapshotter) constructImageBlobURL(ref string) (string, error) {
	refspec, err := reference.Parse(ref)
	if err != nil {
		return "", errors.Wrapf(err, "invalid repo url %s", ref)
	}

	host := refspec.Hostname()
	repo := strings.TrimPrefix(refspec.Locator, host+"/")
	return "https://" + path.Join(host, "v2", repo) + "/blobs", nil
}

// atomicWriteBackingStoreAndTargetConfig
func (o *snapshotter) atomicWriteBackingStoreAndTargetConfig(ctx context.Context, snID string, snKey string, configJSON OverlayBDBSConfig) error {
	data, err := json.Marshal(configJSON)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal %+v configJSON into JSON", configJSON)
	}

	confPath := o.overlaybdConfPath(snID)
	if err := continuity.AtomicWriteFile(confPath, data, 0600); err != nil {
		return errors.Wrapf(err, "failed to commit the OverlayBD config on %s", confPath)
	}
	return nil
}

// prepareWritableOverlaybd
func (o *snapshotter) prepareWritableOverlaybd(ctx context.Context, snID, parentSnID string) error {
	binpath := filepath.Join(o.config.OverlayBDUtilBinDir, "overlaybd-create")

	// TODO(fuweid): 256GB can be configurable?
	out, err := exec.CommandContext(ctx, binpath,
		o.overlaybdWritableDataPath(snID),
		o.overlaybdWritableIndexPath(snID), "64").CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "failed to prepare writable overlaybd: %s", out)
	}
	return nil
}

// commitWritableOverlaybd
func (o *snapshotter) commitWritableOverlaybd(ctx context.Context, snID string) (retErr error) {
	binpath := filepath.Join(o.config.OverlayBDUtilBinDir, "overlaybd-commit")
	tmpPath := filepath.Join(o.root, "snapshots", snID, "block", ".commit-before-zfile")

	out, err := exec.CommandContext(ctx, binpath,
		o.overlaybdWritableDataPath(snID),
		o.overlaybdWritableIndexPath(snID), tmpPath).CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "failed to commit writable overlaybd: %s", out)
	}

	defer func() {
		os.Remove(tmpPath)
	}()

	binpath = filepath.Join(o.config.OverlayBDUtilBinDir, "overlaybd-zfile")
	out, err = exec.CommandContext(ctx, binpath, tmpPath, o.magicFilePath(snID)).CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "failed to create zfile: %s", out)
	}
	return nil
}

// checkFstypeFromDevice return the filesystem type for /dev/xyz.
//
// TODO(fuweid): It will be good to read /dev/xyz to parse the filesystem.
func checkFstypeFromDevice(ctx context.Context, device string) (string, error) {
	fstype, err := exec.CommandContext(ctx, "lsblk",
		"-f",
		"-o", "FSTYPE", // only show the filesystem
		"-n", // no table heading
		device).CombinedOutput()
	if err != nil {
		return "", errors.Wrapf(err, "failed to get filesystem type for %v: %v", device, string(fstype))
	}
	return strings.TrimSpace(string(fstype)), nil
}

// TODO: use device number to check?
func lookup(dir string) error {
	dir = filepath.Clean(dir)

	m, err := mountinfo.GetMounts(mountinfo.SingleEntryFilter(dir))
	if err != nil {
		return errors.Wrapf(err, "failed to find the mount info for %q", dir)
	}

	if len(m) == 0 {
		return errors.Errorf("failed to find the mount info for %q", dir)
	}
	return nil
}

func rollbackContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.TODO(), defaultRollbackTimeout)
}
