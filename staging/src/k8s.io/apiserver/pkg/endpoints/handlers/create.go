/*
Copyright 2017 The Kubernetes Authors.

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

package handlers

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/dryrun"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utiltrace "k8s.io/utils/trace"
)

var namespaceGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}

//@bg: 초기화 과정에서 아래 함수가 실제 리소스 생성시 사용될 함수로 등록되는것으로 추정된다.
func createHandler(r rest.NamedCreater, scope *RequestScope, admit admission.Interface, includeName bool) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		// For performance tracking purposes.
		trace := utiltrace.New("Create", traceFields(req)...)
		defer trace.LogIfLong(500 * time.Millisecond)

		//@bg: dryrun관련정보 https://github.com/k8s-dig/kubernetes/blob/4a3b558c52eb6995b3c5c1db5e54111bd0645a64/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/create.go#L50
		if isDryRun(req.URL) && !utilfeature.DefaultFeatureGate.Enabled(features.DryRun) {
			scope.err(errors.NewBadRequest("the dryRun feature is disabled"), w, req)
			return
		}

		//@bg: ScopeNamer Interface위치 -> staging/src/k8s.io/apiserver/pkg/endpoints/handlers/namer.go
		// Name returns the name from the request, and an optional namespace value if this is a namespace
		// scoped call. An error is returned if the name is not available.
		// Name(req *http.Request) (namespace, name string, err error)
		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			//@bg named resource면 true과 그냥 resource면 false이다
			//정확히 이를 나누는 기준이 뭔지 확실히 파악 해보아야겠다.
			//pod에대한 storage interface는 아래 구현부에서 정해지는것으로 보여진다.
			//pkg/registry/core/rest/storage_core.go
			if includeName {
				// name was required, return
				scope.err(err, w, req)
				return
			}

			// otherwise attempt to look up the namespace
			namespace, err = scope.Namer.Namespace(req)
			if err != nil {
				scope.err(err, w, req)
				return
			}
		}

		// enforce a timeout of at most requestTimeoutUpperBound (34s) or less if the user-provided
		// timeout inside the parent context is lower than requestTimeoutUpperBound.
		ctx, cancel := context.WithTimeout(req.Context(), requestTimeoutUpperBound)
		defer cancel()
		//@bg: outputMediaType은 response에 형상 관련된 일련의 정보들을 가지고있다.
		//https://pkg.go.dev/k8s.io/apiserver/pkg/endpoints/handlers/negotiation#MediaTypeOptions
		//@bg: scpre.Sereialize는 특정 미디어 타입(들)에 대한 인코더 디코더를 가져올수있는 인터페이스다
		//https://pkg.go.dev/k8s.io/apimachinery/pkg/runtime#NegotiatedSerializer
		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		gv := scope.Kind.GroupVersion()
		//@bg: streaming은 watch 동작을 의미한다, watch로 request가 올순없으니 false인듯싶다.
		//내부구현은 대략 헤더의 "Content-Type"을 보고 적절한 Serializer를 반환해주는것 같다.
		s, err := negotiation.NegotiateInputSerializer(req, false, scope.Serializer)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		//@bg: serializer와 HubGroupVersion을 보고 Decoder를 결정하는대
		//staging/src/k8s.io/apiserver/pkg/endpoints/installer.go
		//installer쪽을 보면 HGV가 대략 내부에서 사용되는 internal version이다.
		decoder := scope.Serializer.DecoderToVersion(s.Serializer, scope.HubGroupVersion)

		//@bg: limit은 대략 etcd DefaultMaxRequestBytes(1.5mb)의 두배 3Mb
		//staging/src/k8s.io/apiserver/pkg/server/config.go
		// 1.5MB is the recommended client request size in byte
		// the etcd server should accept. See
		// https://github.com/etcd-io/etcd/blob/release-3.4/embed/config.go#L56.
		// A request body might be encoded in json, and is converted to
		// proto when persisted in etcd, so we allow 2x as the largest request
		// body size to be accepted and decoded in a write request.
     	//MaxRequestBodyBytes: int64(3 * 1024 * 1024),
		body, err := limitedReadBody(req, scope.MaxRequestBodyBytes)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		options := &metav1.CreateOptions{}
		values := req.URL.Query()
		//@bg: DecodeParameters는 src/dest GroupVersion을 비교한뒤 최종적으로 dest groupversion에 맞는 obj를 생성해준다.
		//staging/src/k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme/register_test.go
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(values, scope.MetaGroupVersion, options); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}
		if errs := validation.ValidateCreateOptions(options); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "CreateOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}
		//@bg: Group: meta.k8s.io Version: v1  Kind: CreateOptions
		options.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("CreateOptions"))

		defaultGVK := scope.Kind
		//@bg: 특정 리소스에 해당하는 Storage Object를 신규로 생성
		//input으로 들어온 rest.NamedCreater를 보면 아래 인터페이스가 있는대 New()로 Create에 사용할 obj를 만든다.
		//New() runtime.Object
		//Create(ctx...)
		//실제 위 인터페이스들은 pkg/registry/core/.../storage/storage.go 같은곳 에서 구현하고있다.
		//다만 Service쪽 정도만 위 인터페이스를 바로 구현하고 나머지들은 거의다 genericregistry.store와 strategy를 통해 구현되어있다.
		//여튼 저 storage쪽 구현을 보면 실제 pod같은 object 생성 및 업데이트시 validation을 수행하는 부분이 호출된다.
		//https://pkg.go.dev/k8s.io/apiserver/pkg/registry/generic/registry#Store

		//Service
		//https://pkg.go.dev/k8s.io/kubernetes/pkg/registry/core/service/storage#REST.New
		//https://pkg.go.dev/k8s.io/kubernetes/pkg/registry/core/service/storage#REST.Create

		//Pod
	    //https://pkg.go.dev/k8s.io/kubernetes/pkg/registry/core/pod/storage#NewStorage
		//https://pkg.go.dev/k8s.io/kubernetes/pkg/registry/core/pod#Strategy
		//pkg/registry/core/pod/strategy.go:104
		//@bg: 실제 pod생성 및 업데이트시 spec validation을 수행하는부분
		//pkg/apis/core/validation/validation.go:3229
		original := r.New()
		trace.Step("About to convert to expected version")
		//@bg: 최종적으로 internal 용으로 사용되는 gv의 오브젝트로 변환될것으로 예상
		obj, gvk, err := decoder.Decode(body, &defaultGVK, original)
		if err != nil {
			err = transformDecodeError(scope.Typer, err, original, gvk, body)
			scope.err(err, w, req)
			return
		}
		if gvk.GroupVersion() != gv {
			err = errors.NewBadRequest(fmt.Sprintf("the API version in the data (%s) does not match the expected API version (%v)", gvk.GroupVersion().String(), gv.String()))
			scope.err(err, w, req)
			return
		}
		trace.Step("Conversion done")

		// On create, get name from new object if unset
		if len(name) == 0 {
			_, name, _ = scope.Namer.ObjectName(obj)
		}
		if len(namespace) == 0 && *gvk == namespaceGVK {
			namespace = name
		}
		ctx = request.WithNamespace(ctx, namespace)

		ae := request.AuditEventFrom(ctx)
		admit = admission.WithAudit(admit, ae)
		audit.LogRequestObject(ae, obj, scope.Resource, scope.Subresource, scope.Serializer)

		userInfo, _ := request.UserFrom(ctx)

		trace.Step("About to store object in database")
		admissionAttributes := admission.NewAttributesRecord(obj, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Create, options, dryrun.IsDryRun(options.DryRun), userInfo)
		//@bg: 위에서 r.New()로 만든 특정 타입의 object의 Create부분을 호출할 함수를 생성한다.
		requestFunc := func() (runtime.Object, error) {
			return r.Create(
				ctx,
				name,
				obj,
				rest.AdmissionToValidateObjectFunc(admit, admissionAttributes, scope),
				options,
			)
		}
		// Dedup owner references before updating managed fields
		dedupOwnerReferencesAndAddWarning(obj, req.Context(), false)
		//@bg: 자세한 구조는 보지않았지만 go func이랑 channel로 async하게 함수호출 해주는것으로 보임.
		result, err := finishRequest(ctx, func() (runtime.Object, error) {
			if scope.FieldManager != nil {
				liveObj, err := scope.Creater.New(scope.Kind)
				if err != nil {
					return nil, fmt.Errorf("failed to create new object (Create for %v): %v", scope.Kind, err)
				}
				obj = scope.FieldManager.UpdateNoErrors(liveObj, obj, managerOrUserAgent(options.FieldManager, req.UserAgent()))
				admit = fieldmanager.NewManagedFieldsValidatingAdmissionController(admit)
			}
			if mutatingAdmission, ok := admit.(admission.MutationInterface); ok && mutatingAdmission.Handles(admission.Create) {
				if err := mutatingAdmission.Admit(ctx, admissionAttributes, scope); err != nil {
					return nil, err
				}
			}
			// Dedup owner references again after mutating admission happens
			dedupOwnerReferencesAndAddWarning(obj, req.Context(), true)
			//@bg: r.Create(...) 호출
			//실제 내부구현은 위에서 언급하였던것처럼 특정 리소스의 pkg/registry/core/.../storage/storage.go와 strategy를 보아야한다.
			result, err := requestFunc()
			// If the object wasn't committed to storage because it's serialized size was too large,
			// it is safe to remove managedFields (which can be large) and try again.
			if isTooLargeError(err) {
				if accessor, accessorErr := meta.Accessor(obj); accessorErr == nil {
					accessor.SetManagedFields(nil)
					result, err = requestFunc()
				}
			}
			return result, err
		})
		if err != nil {
			scope.err(err, w, req)
			return
		}
		trace.Step("Object stored in database")

		code := http.StatusCreated
		status, ok := result.(*metav1.Status)
		if ok && err == nil && status.Code == 0 {
			status.Code = int32(code)
		}

		//@bg: resp writer에 저 위에서 결졍된 미디어타입(아마도 json)으로 result를 변환해서 반환해 주는것으로 보여진다.
		transformResponseObject(ctx, scope, trace, req, w, code, outputMediaType, result)
	}
}

// CreateNamedResource returns a function that will handle a resource creation with name.
func CreateNamedResource(r rest.NamedCreater, scope *RequestScope, admission admission.Interface) http.HandlerFunc {
	return createHandler(r, scope, admission, true)
}

// CreateResource returns a function that will handle a resource creation.
func CreateResource(r rest.Creater, scope *RequestScope, admission admission.Interface) http.HandlerFunc {
	return createHandler(&namedCreaterAdapter{r}, scope, admission, false)
}

type namedCreaterAdapter struct {
	rest.Creater
}

func (c *namedCreaterAdapter) Create(ctx context.Context, name string, obj runtime.Object, createValidatingAdmission rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	return c.Creater.Create(ctx, obj, createValidatingAdmission, options)
}

// manager is assumed to be already a valid value, we need to make
// userAgent into a valid value too.
func managerOrUserAgent(manager, userAgent string) string {
	if manager != "" {
		return manager
	}
	return prefixFromUserAgent(userAgent)
}

// prefixFromUserAgent takes the characters preceding the first /, quote
// unprintable character and then trim what's beyond the
// FieldManagerMaxLength limit.
func prefixFromUserAgent(u string) string {
	m := strings.Split(u, "/")[0]
	buf := bytes.NewBuffer(nil)
	for _, r := range m {
		// Ignore non-printable characters
		if !unicode.IsPrint(r) {
			continue
		}
		// Only append if we have room for it
		if buf.Len()+utf8.RuneLen(r) > validation.FieldManagerMaxLength {
			break
		}
		buf.WriteRune(r)
	}
	return buf.String()
}
