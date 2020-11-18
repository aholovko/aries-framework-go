/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/


import axios from 'axios';

// All REST endpoints provided by agent controller
const pkgs = {
    didexchange: {
        CreateInvitation: {
            path: "/connections/create-invitation",
            method: "POST"
        },
        ReceiveInvitation: {
            path: "/connections/receive-invitation",
            method: "POST"
        },
        AcceptInvitation: {
            path: "/connections/{id}/accept-invitation",
            method: "POST",
            pathParam:"id"
        },
        AcceptExchangeRequest: {
            path: "/connections/{id}/accept-request",
            method: "POST",
            pathParam:"id"
        },
        CreateImplicitInvitation: {
            path: "/connections/create-implicit-invitation",
            method: "POST"
        },
        SaveConnection: {
            path: "/connections/create",
            method: "POST"
        },
        RemoveConnection: {
            path: "/connections/{id}/remove",
            method: "POST",
            pathParam:"id"
        },
        QueryConnectionByID: {
            path: "/connections/{id}",
            method: "GET",
            pathParam:"id"
        },
        QueryConnections: {
            path: "/connections",
            method: "GET"
        },
    },
    vdr: {
        SaveDID: {
            path: "/vdr/did",
            method: "POST"
        },
        GetDID: {
            path: "/vdr/did/{id}",
            method: "GET",
            pathParam:"id"
        },
        ResolveDID: {
            path: "/vdr/did/resolve/{id}",
            method: "GET",
            pathParam:"id"
        },
        GetDIDs: {
            path: "/vdr/did/records",
            method: "GET",
        },
    },
    messaging: {
        RegisteredServices: {
            path: "/message/services",
            method: "GET"
        },
        RegisterMessageService: {
            path: "/message/register-service",
            method: "POST"
        },
        RegisterHTTPMessageService: {
            path: "/http-over-didcomm/register",
            method: "POST"
        },
        UnregisterMessageService: {
            path: "/message/unregister-service",
            method: "POST"
        },
        SendNewMessage: {
            path: "/message/send",
            method: "POST"
        },
        SendReplyMessage: {
            path: "/message/reply",
            method: "POST"
        },
    },
    mediator: {
        Register: {
            path: "/mediator/register",
            method: "POST"
        },
        Unregister: {
            path: "/mediator/unregister",
            method: "DELETE"
        },
        GetConnections: {
            path: "/mediator/connections",
            method: "GET"
        },
        Reconnect: {
            path: "/mediator/reconnect",
            method: "POST"
        },
        Status: {
            path: "/mediator/status",
            method: "POST"
        },
        BatchPickup: {
            path: "/mediator/batchpickup",
            method: "POST"
        },
        ReconnectAll: {
            path: "/mediator/reconnect-all",
            method: "GET",
        },
    },
    verifiable: {
        ValidateCredential: {
            path: "/verifiable/credential/validate",
            method: "POST"
        },
        SaveCredential: {
            path: "/verifiable/credential",
            method: "POST"
        },
        GetCredential: {
            path: "/verifiable/credential/{id}",
            method: "GET",
            pathParam:"id"
        },
        GetCredentialByName: {
            path: "/verifiable/credential/name/{name}",
            method: "GET",
            pathParam:"name"
        },
        GetCredentials: {
            path: "/verifiable/credentials",
            method: "GET",
        },
        SignCredential: {
            path: "/verifiable/signcredential",
            method: "POST"
        },
        GeneratePresentation: {
            path: "/verifiable/presentation/generate",
            method: "POST"
        },
        GeneratePresentationByID: {
            path: "/verifiable/credential/{id}/presentation",
            method: "GET",
            pathParam:"id"
        },
        SavePresentation: {
            path: "/verifiable/presentation",
            method: "POST"
        },
        GetPresentation: {
            path: "/verifiable/presentation/{id}",
            method: "GET",
            pathParam: "id"
        },
        GetPresentations: {
            path: "/verifiable/presentations",
            method: "GET",
        },
    },
    introduce:{
        Actions: {
            path: "/introduce/actions",
            method: "GET"
        },
        SendProposal: {
            path: "/introduce/send-proposal",
            method: "POST",
        },
        AcceptProblemReport: {
            path: "/introduce/{piid}/accept-problem-report",
            method: "POST",
            pathParam:"piid"
        },
        SendProposalWithOOBRequest: {
            path: "/introduce/send-proposal-with-oob-request",
            method: "POST",
        },
        SendRequest: {
            path: "/introduce/send-request",
            method: "POST",
        },
        AcceptProposalWithOOBRequest: {
            path: "/introduce/{piid}/accept-proposal-with-oob-request",
            method: "POST",
            pathParam: "piid"
        },
        AcceptProposal: {
            path: "/introduce/{piid}/accept-proposal",
            method: "POST",
            pathParam: "piid"
        },
        AcceptRequestWithPublicOOBRequest: {
            path: "/introduce/{piid}/accept-request-with-public-oob-request",
            method: "POST",
            pathParam: "piid"
        },
        AcceptRequestWithRecipients: {
            path: "/introduce/{piid}/accept-request-with-recipients",
            method: "POST",
            pathParam: "piid"
        },
        DeclineProposal: {
            path: "/introduce/{piid}/decline-proposal",
            method: "POST",
            pathParam: "piid"
        },
        DeclineRequest: {
            path: "/introduce/{piid}/decline-request",
            method: "POST",
            pathParam: "piid"
        },
    },
    outofband: {
        Actions: {
            path: "/outofband/actions",
            method: "GET"
        },
        ActionContinue: {
            path: "/outofband/{piid}/action-continue",
            method: "POST",
            pathParam: "piid"
        },
        ActionStop: {
            path: "/outofband/{piid}/action-stop",
            method: "POST",
            pathParam: "piid"
        },
        CreateRequest: {
            path: "/outofband/create-request",
            method: "POST",
        },
        CreateInvitation: {
            path: "/outofband/create-invitation",
            method: "POST",
        },
        AcceptRequest: {
            path: "/outofband/accept-request",
            method: "POST",
        },
        AcceptInvitation: {
            path: "/outofband/accept-invitation",
            method: "POST",
        },
    },
    issuecredential: {
        Actions: {
            path: "/issuecredential/actions",
            method: "GET",
        },
        SendOffer: {
            path: "/issuecredential/send-offer",
            method: "POST",
        },
        SendProposal: {
            path: "/issuecredential/send-proposal",
            method: "POST",
        },
        SendRequest: {
            path: "/issuecredential/send-request",
            method: "POST",
        },
        AcceptProposal: {
            path: "/issuecredential/{piid}/accept-proposal",
            method: "POST",
            pathParam:"piid"
        },
        DeclineProposal: {
            path: "/issuecredential/{piid}/decline-proposal",
            method: "POST",
            pathParam:"piid"
        },
        AcceptOffer: {
            path: "/issuecredential/{piid}/accept-offer",
            method: "POST",
            pathParam:"piid"
        },
        AcceptProblemReport: {
            path: "/issuecredential/{piid}/accept-problem-report",
            method: "POST",
            pathParam:"piid"
        },
        DeclineOffer: {
            path: "/issuecredential/{piid}/decline-offer",
            method: "POST",
            pathParam:"piid"
        },
        NegotiateProposal: {
            path: "/issuecredential/{piid}/negotiate-proposal",
            method: "POST",
            pathParam:"piid"
        },
        AcceptRequest: {
            path: "/issuecredential/{piid}/accept-request",
            method: "POST",
            pathParam:"piid"
        },
        DeclineRequest: {
            path: "/issuecredential/{piid}/decline-request",
            method: "POST",
            pathParam:"piid"
        },
        AcceptCredential: {
            path: "/issuecredential/{piid}/accept-credential",
            method: "POST",
            pathParam:"piid"
        },
        DeclineCredential: {
            path: "/issuecredential/{piid}/decline-credential",
            method: "POST",
            pathParam:"piid"
        },
    },
    presentproof:{
        Actions: {
            path: "/presentproof/actions",
            method: "GET",
        },
        SendRequestPresentation: {
            path: "/presentproof/send-request-presentation",
            method: "POST",
        },
        SendProposePresentation: {
            path: "/presentproof/send-propose-presentation",
            method: "POST",
        },
        AcceptProblemReport: {
            path: "/presentproof/{piid}/accept-problem-report",
            method: "POST",
            pathParam:"piid"
        },
        AcceptRequestPresentation: {
            path: "/presentproof/{piid}/accept-request-presentation",
            method: "POST",
            pathParam:"piid"
        },
        AcceptProposePresentation: {
            path: "/presentproof/{piid}/accept-propose-presentation",
            method: "POST",
            pathParam:"piid"
        },
        AcceptPresentation: {
            path: "/presentproof/{piid}/accept-presentation",
            method: "POST",
            pathParam:"piid"
        },
        NegotiateRequestPresentation: {
            path: "/presentproof/{piid}/negotiate-request-presentation",
            method: "POST",
            pathParam:"piid"
        },
        DeclineRequestPresentation: {
            path: "/presentproof/{piid}/decline-request-presentation",
            method: "POST",
            pathParam:"piid"
        },
        DeclineProposePresentation: {
            path: "/presentproof/{piid}/decline-propose-presentation",
            method: "POST",
            pathParam:"piid"
        },
        DeclinePresentation: {
            path: "/presentproof/{piid}/decline-presentation",
            method: "POST",
            pathParam:"piid"
        },
    },
    kms: {
        CreateKeySet: {
            path: "/kms/keyset",
            method: "POST",
        },
        ImportKey: {
            path: "/kms/import",
            method: "POST",
        }
    },
}

/**
 * Agent rest client for given agent endpoint
 * @param url is rest endpoint url
 * @class
 */
export const Client = class {
    constructor(url, token) {
        this.url = url
        this.token = token
    }

    async handle(request) {
        const r = (pkgs[request.pkg]) ? pkgs[request.pkg][request.fn] : null;
        if (!r) {
            return "unable to find given pkg/fn:"+request.pkg+"/"+request.fn;
        }

        let url =  this.url + r.path
        if (r.pathParam){
            const p = ((request.payload[r.pathParam])) ? (request.payload[r.pathParam]) : "";
            url = this.url + r.path.replace("{"+r.pathParam+"}", p);
        }

        if (r.queryStrings){
            r.queryStrings.forEach(p => {
                url = url.replace("{"+ p +"}", (request.payload[p]) ? request.payload[p] : "");
            })
        }

        console.debug(`[${r.method}] ${url}, request ${JSON.stringify(request.payload)}`)

        let headers = {}
        if (this.token) {
            headers = {
                "Authorization": `Bearer ${this.token}`
            }
        }

        const resp = await axios({
                method: r.method,
                url: url,
                headers: headers,
                data: request.payload,
            });

        return resp.data;
    }
};


